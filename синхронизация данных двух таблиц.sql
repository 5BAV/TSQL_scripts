/*
ПРИМЕР:
	exec dbo.pr_Refreshing_Table_inPacks
		@target_table = ''[database].[schema].[$$$table]''
		,@source_table = ''[linkserver].[database].[schema].[table]''
		,@condition_for_refreshing = ''src.PK_field_1 = trg.[PK field 1] and ''$$'' = trg.[PK field 2]''
		,@target_filter = ''field_3 = 1 or [field_4] < getdate()''
		,@source_filter = ''[field 5] in (1,2,3)''
		,@type_of_refresh = 0
			-- (0) delete+update+insert, (1) update+insert, (3) update only, (4) insert only
		,@exclude_fields = ''field_7|[field 9]''
		,@computed_columns = ''"field 10" = ''''const'''' | [field 11] = src.[field 9]''
		,@pack_size = 4000
		,@ignore_TS_in_target = 0
		,@show_info = 2
			-- (0) никакой информации, (1) в конце одной строкой, (2) только таблица сравнения, (3) подробная информация в процессе выполнения
*/

create or alter proc [dbo].[pr_Refreshing_Table_inPacks]
	 @target_table varchar(max) --целевая таблица, $$ для обозначения однотипных таблиц
	,@source_table varchar(max) --таблица источник, $$ для обозначения однотипных таблиц
	,@condition_for_refreshing varchar(max) --условия(ключ) для соединения таблиц (on)
	,@target_filter varchar(max) = '' --фильтр на целевой таблице (where)
	,@source_filter varchar(max) = '' --фильтр на таблице источнике (where)
	,@type_of_refresh int --тип модификаций целевой таблицы 0=DEL+UPD+INS 1=UPD+INS 3=UPD 4=INS
	,@exclude_fields varchar(max) = '' --список колонок которые не учитываются в процессе, через |
	,@computed_columns varchar(max) = '' --виртуальные определения колонок нужных для процесса
	,@pack_size int = 4000 --размер пачек для итераций модификации
	,@ignore_TS_in_target bit = 1 --игнорировать изменение таймштампа в целевой таблице (если значение было модифицировано во время выполнения)
	,@show_info int = 0 -- вывод отладочной информации 0=никакой информации 1=в конце одной строкой 2=только таблица сравнения 3=подробная информация в процессе выполнения
as
set nocount, xact_abort on
set lock_timeout 120000
set deadlock_priority low

declare
	@trg_table varchar(max)
	,@trg_schema varchar(max)
	,@trg_database varchar(max)
	,@trg_2parts_name varchar(max)
	,@trg_3parts_name varchar(max)
	,@src_table varchar(max)
	,@src_schema varchar(max)
	,@src_database varchar(max)
	,@src_linkserver varchar(max)
	,@src_2parts_name varchar(max)
	,@src_3parts_name varchar(max)
	,@src_4parts_name varchar(max)
	,@trg_has_$$ int = charindex('$$$', @target_table)
	,@src_has_$$ int = charindex('$$$', @source_table)
	,@condition varchar(max)
	,@computed varchar(max)
	,@trg_filter varchar(max)
	,@src_filter varchar(max)
	,@query nvarchar(max) = ''
	,@message varchar(max) = ''
	,@messages varchar(max) = ''
	,@number int = 0
	,@temp_src_table varchar(50) = concat('##SRC', format(sysdatetime(), 'ddHHmmssfffffff'))
	,@comparison_table varchar(50) = concat('##RSLT', format(sysdatetime(), 'ddHHmmssfffffff'))
	,@columns varchar(max) = ''
	,@src_columns varchar(max) = ''
	,@trg_columns varchar(max) = ''
	,@additional_condition varchar(max) = ''
	,@use_db varchar(max) = ''
	,@table_prefix varchar(max)
	,@offset int
	,@row_count int = 0
	,@row_count2 int = 0
	,@t_nl char(2) = char(13) + char(10)
	,@frmt1 varchar(20) = '[HH:mm:ss]'
	,@t_upd varchar(max) = 'UPD'
	,@t_ins varchar(max) = 'INS'
	,@t_del varchar(max) = 'DEL'
	,@t_busy varchar(max) = 'BUSY'
	,@t_pass varchar(max) = 'PASS'
	,@t_ttl varchar(max) = 'TOTAL'

select
	@trg_table = parsename(@target_table, 1)
	,@trg_schema = parsename(@target_table, 2)
	,@trg_database = iif(@trg_table like '#%', 'TEMPDB', parsename(@target_table, 3))
	,@src_table = parsename(@source_table, 1)
	,@src_schema = parsename(@source_table, 2)
	,@src_database = iif(@src_table like '#%', 'TEMPDB', parsename(@source_table, 3))
	,@src_linkserver = parsename(@source_table, 4)
	,@condition = replace(@condition_for_refreshing, '''', '''''')
	,@computed = @computed_columns
	,@trg_filter = @target_filter
	,@src_filter = @source_filter

set @trg_2parts_name = isnull(quotename(@trg_schema) + '.', '') + quotename(@trg_table)
set @trg_3parts_name = isnull(quotename(@trg_database) + '.', '') + iif(@trg_schema is null and @trg_database is not null, '.', '') + @trg_2parts_name
set @src_2parts_name = isnull(quotename(@src_schema) + '.', '') + quotename(@src_table)
set @src_3parts_name = isnull(quotename(@src_database) + '.', '') + iif(@src_schema is null and @src_database is not null, '.', '') + @src_2parts_name
set @src_4parts_name = isnull(quotename(@src_linkserver) + '.', '') + iif(@src_linkserver is not null and @src_database is null, '.', '') + iif(@src_linkserver is not null and @src_database is null and @src_schema is null, '.', '') + @src_3parts_name

if parsename(@target_table, 4) is not null
	throw 50000, 'Целевая таблица должна быть на текущем сервере !', 1

if parsename(@source_table, 4) <> '' and not exists (select * from sys.servers where is_linked = 1 and name = parsename(@source_table, 4))
	throw 50000, 'НЕ найден линк-сервер !', 1

if charindex('trg.', @condition_for_refreshing) = 0 or charindex('src.', @condition_for_refreshing) = 0
	throw 50000, 'Условие соединения не соответствует формату! (src.[] = trg.[])', 1

if @computed_columns <> '' and exists (select * from string_split(@computed_columns, '|') where charindex('=', value) = 0)
	throw 50000, 'Вычисляемые колонки не соответствует формату! ([имя колонки] = выражение)', 1

if @trg_3parts_name = @src_4parts_name
	throw 50000, 'Имена таблиц одинаковые !', 1

if @trg_has_$$ = 0 and object_id(@trg_3parts_name, 'U') is null
	throw 50000, 'НЕ найдена целевая таблица!', 1

if @src_has_$$ = 0
begin
	set @query =  '
		declare @result int
		exec ' + iif(@src_linkserver = '', '', quotename(@src_linkserver) + '..') + 'sys.sp_executesql
			N''set @id = object_id(''''' + @src_3parts_name + ''''', ''''U'''')''
			,N''@id int output''
			,@id = @result output
		
		if @result is null
			throw 50000, ''НЕ найдена таблица источник!'', 1
	'
	exec (@query)
end

begin try
	
--таблица префиксов для однотипных таблиц
	drop table if exists #table_prefixes
	create table #table_prefixes (prefix varchar(10))

	set @use_db = iif(@trg_database <> '', 'use ' + quotename(@trg_database), '')

	set @query = '
		' + @use_db + '
		select left(t.[name], charindex(''$'', t.[name]) - 1)
		from sys.tables as t
		where
			t.[name] like replace(@table, ''$$'', ''%'')
			and t.[schema_id] = schema_id(isnull(@schema, schema_name()))
			and t.[name] <> @table2
			and exists (select * from dbo.[TABLE PREFIXES] as c with(nolock) where c.Blocked = 0 and t.Name like c.Name + ''%'' )'

	if @trg_has_$$ > 0
	begin
		insert into #table_prefixes (prefix)
		exec sys.sp_executesql
			@query
			,N'@table varchar(max), @table2 varchar(max), @schema varchar(max)'
			,@table = @trg_table
			,@table2 = @src_table
			,@schema = @trg_schema
	end

	set @query = '
		exec ' + iif(@src_linkserver is null, '', quotename(@src_linkserver) + '.') + isnull(quotename(@src_database), '') + '.sys.sp_executesql
		N''' + replace(replace(@query, @use_db, ''), '''', '''''') + '''
		,N''@table varchar(max), @table2 varchar(max), @schema varchar(max)''
		,@table = @table
		,@table2 = @table2
		,@schema = @schema'
	
	if @src_has_$$ > 0
	begin
		insert into #table_prefixes (prefix)
		exec sys.sp_executesql
			@query
			,N'@table varchar(max),@table2 varchar(max), @schema varchar(max)'
			,@table = @src_table
			,@table2 = @trg_table
			,@schema = @src_schema
	end

	if @trg_has_$$ > 0 and @src_has_$$ > 0
		with t as (
			select row_number() over(partition by prefix order by (select null)) as rn
			from #table_prefixes
		)
		delete from t
		where rn <> 2

	while (1=1)
	begin

		if exists (select * from #table_prefixes)
		begin
			select top (1) @table_prefix = prefix
			from #table_prefixes
			order by prefix

		select
			@trg_table = replace(parsename(@target_table, 1), '$$', @table_prefix)
			,@src_table = replace(parsename(@source_table, 1), '$$', @table_prefix)
			,@condition = replace(replace(@condition_for_refreshing, '''', ''''''), '$$', @table_prefix)
			,@computed = replace(@computed_columns, '$$', @table_prefix)
			,@trg_filter = replace(@target_filter, '$$', @table_prefix)
			,@src_filter = replace(@source_filter, '$$', @table_prefix)

			set @trg_2parts_name = isnull(quotename(@trg_schema) + '.', '') + quotename(@trg_table)
			set @trg_3parts_name = isnull(quotename(@trg_database) + '.', '') + iif(@trg_schema is null and @trg_database is not null, '.', '') + @trg_2parts_name
			set @src_2parts_name = isnull(quotename(@src_schema) + '.', '') + quotename(@src_table)
			set @src_3parts_name = isnull(quotename(@src_database) + '.', '') + iif(@src_schema is null and @src_database is not null, '.', '') + @src_2parts_name
			set @src_4parts_name = isnull(quotename(@src_linkserver) + '.', '') + iif(@src_linkserver is not null and @src_database is null, '.', '') + iif(@src_linkserver is not null and @src_database is null and @src_schema is null, '.', '') + @src_3parts_name
		end

		set @message = format(getdate(), @frmt1) + formatmessage('"%s" => "%s"', @src_table, @trg_table)

		if @show_info = 3
			raiserror(@message, 10, 1) with nowait
	
--получение колонок участвующих в процессе
		create table #columns (
			[source] char(3)
			,[column] varchar(200)
			,[type] varchar(20)
			,[size] varchar(10)
			,[sorting] varchar(5)
			,[key_ordinal] int
			,[order] int
			,[isin_condition] bit
			,[is_identity] bit
			,[collation] varchar(200)
			,[expression] varchar(max)
		)
		
		set @use_db = iif(@trg_database <> '', 'use ' + quotename(@trg_database), '')
	
		set @query = '
		' + @use_db + '
			select
				@source
				,c.name
				,t.name
				,case
					when t.[name] in (''varchar'', ''char'', ''varbinary'', ''binary'', ''nvarchar'', ''nchar'')
						then iif(c.max_length = -1, ''max'', cast(columnproperty(c.object_id, c.name, ''charmaxlen'') as nvarchar(5)))
					when t.[name] in (''datetime2'', ''time2'', ''datetimeoffset'') 
						then cast(c.scale as nvarchar(5))
					when t.[name] in (''decimal'', ''numeric'')
						then cast(c.[precision] as nvarchar(5)) + '','' + cast(c.scale as nvarchar(5))
					else '''' end
				,case ic.is_descending_key
					when 0 then ''asc''
					when 1 then ''desc''
						else '''' end
				,isnull(ic.key_ordinal, 0)
				,c.column_id
				,iif(charindex(c.name, @condition) > 0, 1, 0)
				,c.is_identity
				,c.collation_name
			from
				sys.columns as c
				inner join sys.types as t
					on c.user_type_id = t.user_type_id
				left join ( sys.indexes as i
				inner join sys.index_columns as ic
					on ic.index_id = i.index_id
					and ic.object_id = i.object_id
					and i.type = 1 )
					on c.column_id = ic.column_id
					and ic.object_id = c.object_id
			where
				c.object_id = object_id(@table, ''U'')
				and c.is_computed = 0
		'
		
		insert into #columns ([source], [column], [type], [size], [sorting], [key_ordinal], [order], [isin_condition], [is_identity], [collation])
		exec sys.sp_executesql
			@query
			,N'@source char(3), @condition varchar(max), @table varchar(max)'
			,@source = 'TRG'
			,@condition = @condition
			,@table = @trg_2parts_name
		
		set @query = '
			exec ' + iif(@src_linkserver is null, '', quotename(@src_linkserver) + '.') + isnull(quotename(@src_database), '') + '.sys.sp_executesql
			N''' + replace(replace(@query, @use_db, ''), '''', '''''') + '''
			,N''@source char(3), @condition varchar(max), @table varchar(max)''
			,@source = @source
			,@condition = @condition
			,@table = @table'
		
		insert into #columns ([source], [column], [type], [size], [sorting], [key_ordinal], [order], [isin_condition], [is_identity], [collation])
		exec sys.sp_executesql
			@query
			,N'@source char(3), @condition varchar(max), @table varchar(max)'
			,@source = 'SRC'
			,@condition = @condition
			,@table = @src_2parts_name
		
		delete from c
		from
			#columns as c
			inner join string_split(@exclude_fields, '|') as s
				on c.[column] = parsename(s.value, 1)
	
		;with c as (
			select
				parsename(trim(left([value], charindex('=', [value])-1)),1) as [column]
				,trim(substring([value], charindex('=', [value]) + 1, len([value]) - charindex('=', [value]))) as [expression]
			from string_split(@computed, '|')
			where [value] <> ''
		)
		select
			c.[column]
			,t.[type]
			,t.[size]
			,'' as [sorting]
			,0 as [key_ordinal]
			,[order].[max] + row_number() over(order by c.[column], c.expression) as [order]
			,0 as [isin_condition]
			,0 as [is_identity]
			,c.[expression]
		into #computed
		from
			c
			inner join #columns as t
				on c.[column] = t.[column]
				and t.[source] = 'TRG'
			cross join (
				select max([order])
				from #columns
				where [source] = 'SRC'
			) as [order]([max])
			
		merge into #columns as col
		using #computed as cmp
			on col.[source] = 'SRC'
			and col.[column] = cmp.[column]
		when matched
		then update set
			col.[expression] = cmp.[expression]
		when not matched by target
		then insert ([source], [column], [type], [size], [sorting], [key_ordinal], [order], [isin_condition], [is_identity], [expression])
		values (
			'SRC'
			,cmp.[column]
			,cmp.[type]
			,cmp.[size]
			,cmp.[sorting]
			,cmp.[key_ordinal]
			,cmp.[order]
			,cmp.[isin_condition]
			,cmp.[is_identity]
			,cmp.[expression]
		);

--создание временной таблицы с данными источника
		set @trg_database = nullif(@trg_database, 'TEMPDB')
		set @src_database = nullif(@src_database, 'TEMPDB')
		set @trg_3parts_name = isnull(quotename(@trg_database) + '.', '') + iif(@trg_schema is null and @trg_database is not null, '.', '') + @trg_2parts_name
		set @src_3parts_name = isnull(quotename(@src_database) + '.', '') + iif(@src_schema is null and @src_database is not null, '.', '') + @src_2parts_name
		set @src_4parts_name = isnull(quotename(@src_linkserver) + '.', '') + iif(@src_linkserver is not null and @src_database is null, '.', '') + iif(@src_linkserver is not null and @src_database is null and @src_schema is null, '.', '') + @src_3parts_name
	
		select @src_columns = string_agg(cast(quotename([column]) as varchar(max)), ',')
		from #columns
		where
			[column] <> 'timestamp'
			and [expression] is null
			and [source] = 'SRC'
		
		set @query = '
			select
				cast(' + iif(exists (select * from #columns where [source] = 'SRC' and [column] = 'timestamp' ),'[timestamp]', '(0x00)') + ' as binary(8)) as [timestamp]
				,' + @src_columns + '
			into ' + @temp_src_table + '
			from ' + @src_4parts_name + ' as src with(nolock)
			' + iif(@src_filter <> '', 'where ' + @src_filter, '') + '
			set @rc = @@rowcount'
		exec sys.sp_executesql
			@query
			,N'@rc int output'
			,@rc = @row_count output
	
		select @src_columns = string_agg(cast(quotename([column]) + ' ' + sorting as varchar(max)), ',') within group(order by [key_ordinal])
		from #columns
		where
			[key_ordinal] > 0
			and [source] = 'SRC'
		
		set @query = '
			create unique clustered index idx1 on ' + @temp_src_table + ' (' + @src_columns + ')'
		exec (@query)
		
--созданеи таблицы сравнения
		select @src_columns = string_agg(cast('src.' + quotename([column]) + ' as [src_' + [column] + ']' as varchar(max)), ',')
		from #columns
		where
			([key_ordinal] > 0 or [isin_condition] > 0)
			and [source] = 'SRC'
		
		select @trg_columns = string_agg(cast('trg.' + quotename([column]) + ' as [trg_' + [column] + ']' as varchar(max)), ',')
		from #columns
		where
			([key_ordinal] > 0 or [isin_condition] > 0)
			and [source] = 'TRG'

		;with clmns as (
			select
				c.[source]
				,iif([type] in ('image', 'text', 'ntext'), 'cast(@@' + quotename(c.[column]) + ' as varbinary(max))', '@@' + quotename(c.[column])) as [column]
				,c.[type]
				,c.[size]
				,c.[sorting]
				,c.[key_ordinal]
				,c.[order]
				,c.[isin_condition]
				,c.[is_identity]
				,c.[collation]
				,c.[expression]
			from #columns as c
		)
		select @additional_condition = string_agg(cast('(' + 
		'(' + replace(t.[column], '@@', 'trg.') + ' <> ' + isnull('(' + s.expression + ')', replace(s.[column], '@@', 'src.') + iif(isnull(t.[collation], '') <> isnull(s.[collation], ''), ' collate ' + t.[collation], ''))
		+ ') or ' + '(iif(' + replace(t.[column], '@@', 'trg.') + ' is null, 1, 0) + iif(' + isnull('(' + s.expression + ')', replace(s.[column], '@@', 'src.')) + ' is null, 1, 0) = 1))' as varchar(max)), ' or ')
		from
			clmns as t
			inner join clmns as s
				on t.[column] = s.[column]
		where
			t.key_ordinal = 0
			and t.is_identity = 0
			and t.[type] <> 'timestamp'
			and s.[source] = 'SRC'
			and t.[source] = 'TRG'

		set @query = '
			;with trg as (
				select
					*
					' + iif(exists (select * from #columns where [source] = 'TRG' and [column] = 'timestamp'), '', ',cast((0x00) as binary(8)) as [timestamp]') + '
				from ' + @trg_3parts_name + ' as trg with(nolock)
				' + iif(@trg_filter <> '', 'where ' + @trg_filter, '') + '
			)
			select
				identity(int) as [order]
				,cast(trg.[timestamp] as varbinary(8)) as [trg_timestamp]
				,' + @trg_columns + '
				,cast(src.[timestamp] as varbinary(8)) as [src_timestamp]
				,' + @src_columns + '
				,case
					when (trg.[timestamp] is not null and src.[timestamp] is not null and trg.[timestamp] < min_active_rowversion()) and (' + @additional_condition + ')
					then ''' + @t_upd + '''
					when trg.[timestamp] is null
					then ''' + @t_ins + '''
					when src.[timestamp] is null and trg.[timestamp] < min_active_rowversion()
					then ''' + @t_del + '''
					when trg.[timestamp] >= min_active_rowversion()
					then ''' + @t_busy + '''
					else ''' + @t_pass + '''
						end as [command]
			into ' + @comparison_table + '
			from
				trg
				full join ' + @temp_src_table + ' as src
					on ' + replace(@condition, '''''', '''') + '
			order by [command]'
		
		exec (@query)
			
		set @query = 'create clustered index idx2 on ' + @comparison_table + ' ([command], [order])'
		exec (@query)
		
		create table #r (
			cmd varchar(100)
			,cnt int
			,cnt2 int
		)

		set @query = '
			select
				[command]
				,count(*)
			from ' + @comparison_table + '
			group by rollup([command])'
	
		insert into #r (cmd, cnt)
		exec (@query)
	
		select
			@message = string_agg(concat(char(9), case t.c
					when @t_ttl then 'Всего строк'
					when @t_upd then 'На обновление'
					when @t_ins then 'На вставку'
					when @t_del then 'На удаление'
					when @t_busy then 'Занято транз-ей'
						else 'Не требуют обработки' end, ': ',isnull(r.cnt, 0)), @t_nl)
		from
			(values (@t_ttl), (@t_upd), (@t_ins), (@t_del), (@t_busy), (@t_pass)) as t(c)
			left join #r as r
				on t.c = isnull(r.cmd, @t_ttl)

		set @message = format(getdate(), @frmt1) + 'Результат сравнения: ' + @t_nl + @message

		if @show_info = 3
			raiserror(@message, 10, 1) with nowait
		else if @show_info = 2
		begin
			set @query = 'select * from ' + @comparison_table + ' order by [command], [order]'
			exec (@query)
			return
		end
	
--опция UPD
		if @type_of_refresh in (0, 1, 3)
		begin
			select @columns = string_agg(cast(concat('trg.', quotename(t.[column])) + ' = ' + isnull(s.expression, concat('src.', quotename(s.[column]))) as varchar(max)), ',')
			from
				#columns as t
				inner join #columns as s
					on t.[column] = s.[column]
			where
				t.key_ordinal = 0
				and t.is_identity = 0
				and t.[type] <> 'timestamp'
				and s.[source] = 'SRC'
				and t.[source] = 'TRG'
				
			select @src_columns = string_agg(cast('tmp.' + quotename('src_' + l.[column]) + ' = ' + 'src.' + quotename(l.[column]) + isnull(' collate ' + r.[collation], '') as varchar(max)), ' and ')
			from
				#columns as l
				left join #columns as r
					on r.[column] = l.[column]
					and r.[source] = 'TRG'
					and r.expression is null
					and isnull(r.[collation], '') <> isnull(l.[collation], '')
			where
				(l.[key_ordinal] > 0 or l.[isin_condition] > 0)
				and l.[source] = 'SRC'
			
			select @trg_columns = string_agg(cast('tmp.' + quotename('trg_' + [column]) + ' = ' + 'trg.' + quotename([column]) as varchar(max)), ' and ')
			from #columns
			where
				[source] = 'TRG'
				and ([key_ordinal] > 0 or [isin_condition] > 0
					or (@ignore_TS_in_target = 0 and [column] = 'timestamp'))
	
			set @query = '
				select
					@min = min([order])
					,@max = max([order])
				from ' + @comparison_table + '
				where [command] = ''' + @t_upd + '''
			'
			exec sys.sp_executesql
				@query
				,N'@min int output, @max int output'
				,@min = @offset output
				,@max = @number output
		
			set @row_count = 0
	
			while (@offset <= @number)
			begin
				set @query = '
					update trg set
						' + @columns + '
					from
						' + @temp_src_table + ' as src
						inner join ' + @comparison_table + ' as tmp
							on ' + @src_columns + '
						inner join ' + @trg_3parts_name + ' as trg
							on ' + @trg_columns + '
					where
						tmp.[command] = ''' + @t_upd + '''
						and tmp.[order] between ' + cast(@offset as varchar(max)) + ' and ' + cast((@offset + @pack_size - 1) as varchar(max)) + '
					set @rc = @@rowcount
				'
				exec sys.sp_executesql
					@query
					,N'@rc int output'
					,@rc = @row_count2 output
	
				select
					@offset += @pack_size
					,@row_count += @row_count2
			end
	
			update #r set
				cnt2 = @row_count
			where cmd = @t_upd
		
			set @message = format(getdate(), @frmt1) + concat('Обновлено строк: ', @row_count)

			if @show_info = 3
				raiserror(@message, 10, 1) with nowait
		end
		
--опция INS
		if @type_of_refresh in (0, 1, 4)
		begin
			
			select 
				@trg_columns = string_agg(cast(quotename(t.[column]) as varchar(max)), ',')
				,@columns = string_agg(cast(isnull('(' + s.expression + ')', concat('src.', quotename(s.[column]))) as varchar(max)), ',')
			from
				#columns as t
				inner join #columns as s
					on t.[column] = s.[column]
					and s.[source] = 'SRC'
			where 
				t.[type] <> 'timestamp'
				and t.is_identity = 0
				and t.[source] = 'TRG'
		
			select @src_columns = string_agg(cast('tmp.' + quotename('src_' + l.[column]) + ' = ' + 'src.' + quotename(l.[column]) + isnull(' collate ' + r.[collation], '') as varchar(max)), ' and ')
			from
				#columns as l
				left join #columns as r
					on r.[column] = l.[column]
					and r.[source] = 'TRG'
					and r.expression is null
					and isnull(r.[collation], '') <> isnull(l.[collation], '')
			where
				(l.[key_ordinal] > 0 or l.[isin_condition] > 0)
				and l.[source] = 'SRC'

			set @query = '
				select
					@min = min([order])
					,@max = max([order])
				from ' + @comparison_table + '
				where [command] = ''' + @t_ins + '''
			'
			
			exec sys.sp_executesql
				@query
				,N'@min int output, @max int output'
				,@min = @offset output
				,@max = @number output
	
			set @row_count = 0
	
			while (@offset <= @number)
			begin
				set @query = '
					insert into ' + @trg_3parts_name + ' (' + @trg_columns + ')
					select
						' + @columns + '
					from
						' + @temp_src_table + ' as src
						inner join ' + @comparison_table + ' as tmp
							on ' + @src_columns + '
					where
						tmp.[command] = ''' + @t_ins + '''
						and tmp.[order] between ' + cast(@offset as varchar(max)) + ' and ' + cast((@offset + @pack_size - 1) as varchar(max)) + '
					set @rc = @@rowcount
				'
				exec sys.sp_executesql
					@query
					,N'@rc int output'
					,@rc = @row_count2 output
		
				select
					@offset += @pack_size
					,@row_count += @row_count2
			end
	
			update #r set
				cnt2 = @row_count
			where cmd = @t_ins

			set @message = format(getdate(), @frmt1) + concat('Вставлено строк: ', @row_count)

			if @show_info = 3
				raiserror(@message, 10, 1) with nowait

		end
		
--опция DEL
		if @type_of_refresh in (0)
		begin
			
			select @trg_columns = string_agg(cast('tmp.' + quotename('trg_' + [column]) + ' = ' + 'trg.' + quotename([column]) as varchar(max)), ' and ')
			from #columns
			where
				[source] = 'TRG'
				and ([key_ordinal] > 0 or [isin_condition] > 0
					or (@ignore_TS_in_target = 0 and [column] = 'timestamp'))
		
			set @query = '
				select
					@min = min([order])
					,@max = max([order])
				from ' + @comparison_table + '
				where [command] = ''' + @t_del + '''
			'
			exec sys.sp_executesql
				@query
				,N'@min int output, @max int output'
				,@min = @offset output
				,@max = @number output
	
			set @row_count = 0
		
			while (@offset <= @number)
			begin
				set @query = '
					delete from trg
					from
						' + @trg_3parts_name + ' as trg
						inner join ' + @comparison_table + ' as tmp
							on ' + @trg_columns + '
					where
						tmp.[command] = ''' + @t_del + '''
						and tmp.[order] between ' + cast(@offset as varchar(max)) + ' and ' + cast((@offset + @pack_size - 1) as varchar(max)) + '
					set @rc = @@rowcount
				'
				exec sys.sp_executesql
					@query
					,N'@rc int output'
					,@rc = @row_count2 output
		
				select
					@offset += @pack_size
					,@row_count += @row_count2
			end
	
			update #r set
				cnt2 = @row_count
			where cmd = @t_del

			set @message = format(getdate(), @frmt1) + concat('Удалено строк: ', @row_count)

			if @show_info = 3
				raiserror(@message, 10, 1) with nowait
		
		end

		delete from #table_prefixes
		where prefix = @table_prefix

		select @messages += quotename(@src_table) + '=>' + quotename(@trg_table) + ' ' + string_agg(concat(isnull(r.cmd, @t_ttl), ':', r.cnt, quotename(r.cnt2,'(') ), ', ') + @t_nl
		from #r as r

		if not exists (select * from #table_prefixes)
			break

		drop table if exists #columns
		drop table if exists #computed
		drop table if exists #r

		set @query = '
			drop table if exists ' + @temp_src_table + '
			drop table if exists ' + @comparison_table
		
		exec (@query)

	end 

end try
begin catch
	if @query <> ''
	begin
		print 'LAST QUERY'
		print @query--exec dbo.LongPrint @query
	end
	;throw
end catch

if @show_info = 1
	print @messages

go
