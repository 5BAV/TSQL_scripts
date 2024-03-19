
--скрипт запускается на сервере приемнике, который должен быть смапен по базам с источником (в таблице #db_list)
--при @ops = 0 генерируется скрипт на устранение различий в схемах объектов, запускается отдельно, возможен выбор объектов
--при @ops = 1 выполняется накатывание всех найденных различий

set nocount, xact_abort on

declare
-- 0=только сгенерировать скрипт, 1=устранить все различия
	@ops int = 0
-- фильтр для таблиц (разделитель | )
	,@exclude_tables nvarchar(max) = ''
-- фильтр для индексов (разделитель | )
	,@exclude_indexes nvarchar(max) = ''
-- фильтр для модулей (разделитель | )
	,@exclude_modules nvarchar(max) = ''
-- фильтр для ограничений (разделитель | )
	,@exclude_defaults nvarchar(max) = ''
--минимальное значение даты принятое в системе	
	,@min_year nvarchar(4) = '1900'
--в какой базе хранить метаданные
	,@metadata_db sysname = 'TempDB'

--перечень серверов и их баз данных на которых возможен запуск скрипта
drop table if exists #db_list
select *
into #db_list
from (values
	('сервер источник 1',		'база данных 1',			'сервер приемник 1',		'файловая группа')
	,('сервер источник 1',		'база данных 2',			'сервер приемник 1',		'файловая группа')
	,('сервер источник 2',		'база данных 3',			'сервер приемник 2',		'файловая группа')
	,('сервер источник 3',		'база данных 4',			'сервер приемник 2',		'файловая группа')
) as t (src_srv, db, trg_srv, trg_filegr)

--*******************************************************************************************************--
declare
	@query nvarchar(max)
	,@applock_const nvarchar(50) = 'SYNCHRO'
	,@result int
	,@user sysname = system_user
	,@message nvarchar(max) = ''
	,@source_server sysname
	,@db sysname
	,@sysTable sysname
	,@target_filegroup sysname
	,@current_step nvarchar(10)
	,@step_desc nvarchar(max)
	,@sql_text nvarchar(max)
	,@sql_text2 nvarchar(max)
	,@synch_result_tbl sysname = @metadata_db + '..synch_result_' + format(getdate(), 'HHmmss')

if (@@servername like '%PROD%')
	throw 50000, 'СКРИПТ НЕ ДЛЯ ПРОДА !', 1

if cast(serverproperty('ProductMajorVersion') as int) < 14
	throw 50000, 'ВЕРСИЯ СЕРВЕРА МЛАДШЕ ДОПУСТИМОЙ !', 1

if exists (select * from #db_list where db = @metadata_db)
	throw 50000, 'ДЛЯ ХРАНЕНИЯ МЕТАДАННЫХ УКАЖИТЕ БД КОТОРАЯ НА УЧАСТВУЕТ В СИНХРОНИЗАЦИИ ДАННЫХ !', 1

if exists (select * from #db_list where db = 'tempdb')
	throw 50000, 'TEMPDB НЕ ДОЛЖНА УЧАВСТВОВАТЬ В СИНХРОНИЗАЦИИ ДАННЫХ !', 1

declare
	@c_nl char(2) = char(13)+char(10)
	,@t_source nvarchar(10) = 'source'
	,@t_target nvarchar(10) = 'target'
	,@t_schema nvarchar(10) = 'schema'
	,@t_table nvarchar(10) = 'table'
	,@t_column nvarchar(10) = 'column'
	,@t_index nvarchar(10) = 'index'
	,@t_constraint nvarchar(10) = 'constraint'
	,@t_function nvarchar(10) = 'function'
	,@t_procedure nvarchar(10) = 'procedure'
	,@t_trigger nvarchar(10) = 'trigger'
	,@t_view nvarchar(10) = 'view'
	,@t_synonym nvarchar(10) = 'synonym'
	,@t_sequence nvarchar(10) = 'sequence'
	,@t_partition_function nvarchar(20) = 'partition_function'
	,@t_partition_scheme nvarchar(20) = 'partition_scheme'
	,@t_create nvarchar(10) = 'create'
	,@t_drop nvarchar(10) = 'drop'
	,@t_alter nvarchar(10) = 'alter'
	,@marker nvarchar(10) = ';;;'

select @exclude_tables = string_agg(concat('t.[name] like ', quotename(trim([value]), '''')), ' or ')
from string_split(@exclude_tables, '|')
where [value] <> ''

select @exclude_indexes = string_agg(concat('i.[name] like ', quotename(trim([value]), '''')), ' or ')
from string_split(@exclude_indexes, '|')
where [value] <> ''

select @exclude_modules = string_agg(concat('o.[name] like ', quotename(trim([value]), '''')), ' or ')
from string_split(@exclude_modules, '|')
where [value] <> ''

select @exclude_defaults = string_agg(concat('df.[name] like ', quotename(trim([value]), '''')), ' or ')
from string_split(@exclude_defaults, '|')
where [value] <> ''

set @metadata_db = quotename(parsename(@metadata_db,1))

exec @result = sys.sp_getapplock
	@Resource = @applock_const
	,@LockMode = 'exclusive'
	,@LockOwner = 'session'
	,@LockTimeout = 0
	,@DbPrincipal = @user

if @result < 0
begin
    select @message = string_agg(request_session_id, ',')
	from sys.dm_tran_locks (nolock)
	where
		resource_type = 'APPLICATION'
		and request_owner_type = 'SESSION'
		and resource_description like '%' + @applock_const + '%'

	set @message = concat('ЗАКРОЙТЕ СЕССИИ: ', @message, ' !')
	
	;throw 50000, @message, 1
end

drop table if exists [#SCRIPT_TABLE]

create table [#SCRIPT_TABLE] (
	db sysname not null
	,current_step int not null
	,step_desc nvarchar(max) not null
	,sql_text nvarchar(max) not null
	,object_desc nvarchar(128) not null
	,index ix_db clustered (db)
)

declare cursDB cursor for
	select
		src_srv
		,db
		,trg_filegr
	from #db_list
	where @@servername like '%' + trg_srv + '%'
open cursDB
fetch next from cursDB into @source_server, @db, @target_filegroup
while @@fetch_status = 0
begin

	begin--получение метаданных

	drop table if exists [#SYS_schemas]
	drop table if exists [#SYS_tables]
	drop table if exists [#SYS_columns]
	drop table if exists [#SYS_columns_alter]
	drop table if exists [#SYS_indexes]
	drop table if exists [#SYS_constraints]
	drop table if exists [#SYS_modules]
	drop table if exists [#SYS_synonyms]
	drop table if exists [#SYS_sequences]
	drop table if exists [#SYS_partition_schemes]
	drop table if exists [#SYS_partition_functions]
	drop table if exists [#SYS_partition_alter]

	create table [#SYS_schemas] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[name] nvarchar(128) not null
		,[purpose.id] as concat(purpose, ':', [id]) persisted not null
	)
	
	create table [#SYS_tables] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[schema] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,[is filetable] bit
		,[is in-memory] bit
		,[durability] nvarchar(128)
		,[is external] bit
		,[is node] bit
		,[is edge] bit
		,[is encrypted] bit
		,[is fake] bit
		,[is temporal] bit
		,[lock escalation] nvarchar(128)
		,[schema.table] as ([schema] + '.' + [name]) persisted not null
		,[purpose.id] as concat(purpose, ':', [id]) persisted not null
		,[not supported] as ([is filetable] | [is in-memory] | [is external] | [is node] | [is edge] | [is encrypted] | [is fake] | [is temporal])
	)
	
	create table [#SYS_columns] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[table id] int not null
		,[name] nvarchar(128) not null
		,[ordinal position] int
		,[is nullable] bit
		,[is rowguidcol] bit
		,[is identity] bit
		,[is filestream] bit
		,[is xml_document] bit
		,[is sparse] bit
		,[is column_set] bit
		,[is hidden] bit
		,[is masked] bit
		,[seed value] bigint
		,[increment value] bigint
		,[last value] bigint
		,[is computed] bit
		,[definition for computed] nvarchar(max)
		,[is persisted] bit
		,[collation] nvarchar(128)
		,[type name] nvarchar(128)
		,[type size] nvarchar(128)
		,[default for type] nvarchar(128)
		,[purpose.table id] as concat(purpose, ':', [table id]) persisted not null
	)
	
	create table [#SYS_columns_alter] (
		[schema.table] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,primary key ([schema.table], [name])
	)
	
	create table [#SYS_indexes] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[table id] int not null
		,[name] nvarchar(128)
		,[key column names] nvarchar(max)
		,[included column names] nvarchar(max)
		,[index type] nvarchar(128)
		,[ignore dup key] bit
		,[is unique] bit
		,[is padded] bit
		,[is disabled] bit
		,[is hypothetical] bit
		,[fill factor] int
		,[allow row locks] bit
		,[allow page locks] bit
		,[optimize for sequential key] bit
		,[has filter] bit
		,[filter definition] nvarchar(max)
		,[is primary key] bit
		,[is unique constraint] bit
		,[rowcount] bigint
		,[data_size] bigint
		,[data space id] int not null
		,[partition scheme] nvarchar(128)
		,[partition column] nvarchar(128)
		,[purpose.table id] as concat(purpose, ':', [table id]) persisted not null
	)
	
	create table [#SYS_constraints] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[table id] int not null
		,[name] nvarchar(128) not null
		,[column names] nvarchar(max)
		,[type] nvarchar(128)
		,[definition] nvarchar(max)
		,[definition_full] nvarchar(max)
		,[purpose.table id] as concat(purpose, ':', [table id]) persisted not null
	)
	
	create table [#SYS_modules] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[schema] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,[object type] nvarchar(2) not null
		,[is trg disabled] bit
		,[is encrypted] bit
		,[is binding] bit
		,[definition] nvarchar(max)
		,[type desc] nvarchar(128)
		,[parent id] int
		,[parent schema] nvarchar(128)
		,[parent name] nvarchar(128)
		,[schema.object] as ([schema] + '.' + [name]) persisted not null
		,[parent schema.object] as ([parent schema] + '.' + [parent name])
		,[purpose.id] as concat(purpose, ':', [id]) persisted not null
	)
	
	create table [#SYS_synonyms] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[schema] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,[base object name] nvarchar(2000)
		,[schema.object] as ([schema] + '.' + [name]) persisted not null
		,[purpose.id] as concat(purpose, ':', [id]) persisted not null
	)
	
	create table [#SYS_sequences] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[schema] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,[type] nvarchar(128)
		,[start value] bigint
		,[increment] bigint
		,[minimum value] bigint
		,[maximum value] bigint
		,[is cycling] bit
		,[is cached] bit
		,[cache size] int
		,[last used value] bigint
		,[schema.object] as ([schema] + '.' + [name]) persisted not null
		,[purpose.id] as concat(purpose, ':', [id]) persisted not null
	)
	
	create table [#SYS_partition_schemes] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,[function id] int not null
		,[data space id] int not null
		,[definition] nvarchar(max) not null
		,[definition checksum] as checksum(upper(replace(replace(replace(replace([definition],char(32),''),char(9),''),char(10),''),char(13),''))) persisted not null
		,[purpose.name] as concat(purpose, ':', [name]) persisted not null
	)

	create table [#SYS_partition_functions] (
		[purpose] nvarchar(10) not null
		,[server] nvarchar(128) not null
		,[db] nvarchar(128) not null
		,[id] int not null
		,[name] nvarchar(128) not null
		,[data space id] int not null
		,[definition] nvarchar(max) not null
		,[definition checksum] as checksum(upper(replace(replace(replace(replace([definition],char(32),''),char(9),''),char(10),''),char(13),''))) persisted not null
		,[purpose.name] as concat(purpose, ':', [name]) persisted not null
	)

	create table [#SYS_partition_alter] (
		[type] nvarchar(128) not null
		,[name] nvarchar(128) not null
		,primary key ([type], [name])
	)

	declare cursMD cursor for
		select [name]
		from (values
			('schemas')
			,('tables')
			,('columns')
			,('objects')
			,('types')
			,('identity_columns')
			,('computed_columns')
			,('indexes')
			,('index_columns')
			,('default_constraints')
			,('check_constraints')
			,('foreign_keys')
			,('foreign_key_columns')
			,('synonyms')
			,('sequences')
			,('sql_modules')
			,('triggers')
			,('partitions')
			,('partition_functions')
			,('partition_schemes')
			,('partition_parameters')
			,('partition_range_values')
			,('data_spaces')
			,('destination_data_spaces')
			,('filegroups')
			,('allocation_units')
		) as sysTables([name])

	open cursMD
	fetch next from cursMD into @sysTable
	while @@fetch_status = 0
	begin
		set @query = '
			drop table if exists ' + @metadata_db + '..SRC_' + @sysTable + '
			select * 
			into ' + @metadata_db + '..SRC_' + @sysTable + '
			from openrowset(
				''SQLNCLI''
				,N''Server=' + @source_server + ';Database=' + @db + ';Trusted_Connection=yes;''
				,''select * from ' + quotename(@db) + '.sys.' + @sysTable + ' with(nolock)'')
		'
		exec(@query)
--print concat('--Получена ', @sysTable, ' (', @@rowcount, ')')
		fetch next from cursMD into @sysTable
	end
	close cursMD
	deallocate cursMD
	
	--схемы
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,s.schema_id as [object id]
			,quotename(s.name)
		from sys.schemas as s
		where
			s.schema_id > 4 
			and s.schema_id < 1000
	'
	insert into [#SYS_schemas] ([purpose],[server],[db],[id],[name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_schemas] ([purpose],[server],[db],[id],[name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server,@purpose = @t_source
	
	--схемы секционирования
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,ps.name
			,pf.function_id
			,ps.data_space_id
			,''create partition scheme '' + quotename(ps.[name]) + '' as partition '' + quotename(pf.[name]) + '' to ('' + stuff(fg.[name].value(''.'', ''nvarchar(max)''), 1, 1, '''') + '')'' as [definition]
		from
			sys.partition_schemes as ps
			inner join sys.partition_functions as pf
				on pf.function_id = ps.function_id
			cross apply (
				select '','' + quotename(/*fg.name*/''default'')
				from
					sys.data_spaces ds
					inner join sys.destination_data_spaces as dds
						on dds.partition_scheme_id = ds.data_space_id
					inner join sys.filegroups as fg
						on fg.data_space_id = dds.data_space_id
				where ps.data_space_id = ds.data_space_id
				order by dds.destination_id
				for xml path(''''), type
			) as fg([name])
	'
	
	insert into [#SYS_partition_schemes] ([purpose],[server],[db],[name],[function id],[data space id],[definition])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_partition_schemes] ([purpose],[server],[db],[name],[function id],[data space id],[definition])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server,@purpose = @t_source

	--функции секционирования
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,pf.function_id
			,pf.[name]
			,ps.data_space_id
			,''create partition function '' + quotename(pf.[name]) + '' ('' + t.[name]
			+ isnull(''('' + case
					when t.[name] in (''varchar'', ''char'', ''varbinary'', ''binary'', ''nvarchar'', ''nchar'')
						then cast(iif(t.name like ''n%'', pp.max_length / 2, pp.max_length) as nvarchar(10))
					when t.[name] in (''datetime2'', ''time2'', ''datetimeoffset'') 
						then cast(pp.scale as nvarchar(10))
					when t.[name] in (''decimal'', ''numeric'')
						then cast(pp.[precision] as nvarchar(10)) + '','' + cast(pp.scale as nvarchar(10))
					else null end + '')'', '''')
			+ '') '' + ''as range '' + iif(pf.boundary_value_on_right = 1, ''right'', ''left'') + '' for values (''
		    + stuff(rg.[value].value(''.'', ''nvarchar(max)''), 1, 1, '''')
		    + '')'' as [definition]
		from
			sys.partition_functions as pf
			inner join sys.partition_schemes as ps
				on pf.function_id = ps.function_id
			inner join sys.partition_parameters as pp
				on pp.function_id = pf.function_id
			inner join sys.types as t
				on t.user_type_id = pp.user_type_id
			cross apply (
				select
		            '',''
		            + case
		                  when sql_variant_property(r.[value],  ''basetype'') in (''char'', ''varchar'', ''nchar'', ''nvarchar'', ''uniqueidentifier'') 
		                    then quotename(cast(r.[value] as nvarchar(4000)), '''''''')
		                  when sql_variant_property(r.[value], ''basetype'') in (''date'', ''datetime'', ''smalldatetime'', ''datetime2'', ''datetimeoffset'') 
		                    then quotename(format(cast(r.[value] as datetime2), ''yyyy-MM-ddTHH:mm:ss.fffffff K''), '''''''')
		                  when sql_variant_property(r.[value], ''basetype'') = ''time'' 
		                    then quotename(format(cast(r.[value] as time), ''hh\:mm\:ss\.fffffff''),'''''''')
		                  when sql_variant_property(r.[value], ''basetype'') in (''binary'', ''varbinary'') 
		                    then convert(nvarchar(4000), r.[value], 1)
		                  else cast(r.[value] as nvarchar(4000))
		              end
				from sys.partition_range_values as r
				where pf.[function_id] = r.[function_id]
				order by r.boundary_id
				for xml path(''''), type
		    ) as rg([value])		
	'
	
	insert into [#SYS_partition_functions] ([purpose],[server],[db],[id],[name],[data space id],[definition])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_partition_functions] ([purpose],[server],[db],[id],[name],[data space id],[definition])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server,@purpose = @t_source

	--таблицы
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,t.object_id as [table id]
			,quotename(s.name)
			,quotename(t.name)
			,t.is_filetable
			,t.is_memory_optimized
			,concat(t.durability,'' '',quotename(t.durability_desc))
			,t.is_external
			,t.is_node
			,t.is_edge
			,0 --objectproperty(t.object_id,''isencrypted'')
			,0 --objectproperty(t.object_id,''tableisfake'')
			,0 --objectproperty(t.object_id,''tabletemporaltype'')
			,t.lock_escalation_desc
		from
			sys.tables as t
			inner join sys.schemas as s
				on s.schema_id = t.schema_id
		' + iif(@exclude_tables <> '', 'where not (' + @exclude_tables + ')', '')
	
	insert into [#SYS_tables] ([purpose],[server],[db],[id],[schema],[name],[is filetable],[is in-memory],[durability],[is external],[is node],[is edge],[is encrypted],[is fake],[is temporal],[lock escalation])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_tables] ([purpose],[server],[db],[id],[schema],[name],[is filetable],[is in-memory],[durability],[is external],[is node],[is edge],[is encrypted],[is fake],[is temporal],[lock escalation])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server,@purpose = @t_source
	
	--колонки
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,c.object_id as [table id]
			,quotename(c.name)
			,row_number() over(partition by c.object_id order by c.column_id) as [ordinal position] --,columnproperty(c.object_id, c.name, ''ordinal'') as [ordinal position]
			,c.is_nullable
			,c.is_rowguidcol
			,c.is_identity
			,c.is_filestream
			,c.is_xml_document
			,c.is_sparse
			,c.is_column_set
			,c.is_hidden
			,c.is_masked
			,cast(ic.seed_value as bigint)
			,cast(ic.increment_value as bigint)
			,cast(ic.last_value as bigint)
			,c.is_computed
			,isnull(cc.[definition],'''')
			,cc.is_persisted
			,c.collation_name
			,t.name
			,isnull(''('' + case
				when c.is_computed = 1
					then null
				when t.[name] in (''varchar'', ''char'', ''varbinary'', ''binary'', ''nvarchar'', ''nchar'')
					then iif(c.max_length = -1, ''max'', cast(iif(t.name like ''n%'', c.max_length / 2, c.max_length)  as nvarchar(10))) 
				when t.[name] in (''datetime2'', ''time2'', ''datetimeoffset'') 
					then cast(c.scale as nvarchar(10))
				when t.[name] in (''decimal'', ''numeric'')
					then cast(c.[precision] as nvarchar(10)) + '','' + cast(c.scale as nvarchar(10))
				else null end + '')'','''')
			,''('' + case
				when t.[name] in (''image'',''varbinary'',''binary''/*,timestamp*/) then ''0x0''
				when t.[name] in (''uniqueidentifier'') then ''''''00000000-0000-0000-0000-000000000000''''''
				when t.[name] in (''text'',''ntext'',''varchar'',''char'',''nvarchar'',''nchar'',''xml'') then ''''''''''''
				when t.[name] in (''tinyint'',''smallint'',''int'',''real'',''money'',''float'',''bit'',''decimal'',''numeric'',''smallmoney'',''bigint'') then ''0''
				when t.[name] in (''time'',''date'',''datetime2'',''datetimeoffset'',''smalldatetime'',''datetime'') then ''''''' + @min_year + '-01-01''''''
					else '''''''''''' end + '')''
		from
			sys.columns as c
			inner join sys.objects as o
				on c.object_id = o.object_id
				and o.is_ms_shipped = 0
			inner join sys.types as t
				on c.user_type_id = t.user_type_id
			left join sys.identity_columns as ic
				on c.[object_id] = ic.[object_id]
				and c.column_id = ic.column_id
			left join sys.computed_columns as cc
				on c.[object_id] = cc.[object_id]
				and c.column_id = cc.column_id
	'
	
	insert into [#SYS_columns] ([purpose],[server],[db],[table id],[name],[ordinal position],[is nullable],[is rowguidcol],[is identity],[is filestream],[is xml_document],[is sparse],[is column_set],[is hidden],[is masked],[seed value],[increment value],[last value],[is computed],[definition for computed],[is persisted],collation,[type name],[type size],[default for type])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_columns] ([purpose],[server],[db],[table id],[name],[ordinal position],[is nullable],[is rowguidcol],[is identity],[is filestream],[is xml_document],[is sparse],[is column_set],[is hidden],[is masked],[seed value],[increment value],[last value],[is computed],[definition for computed],[is persisted],collation,[type name],[type size],[default for type])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
	
	--индексы
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,i.object_id
			,quotename(i.name)
			,stuff(ic.[key].value(''/x[1]'',''nvarchar(max)''),1,1,'''')
			,isnull(stuff(ic2.[incl].value(''/x[1]'',''nvarchar(max)''),1,1,''''),'''')
			,lower(i.type_desc)
			,i.ignore_dup_key
			,i.is_unique
			,i.is_padded
			,i.is_disabled
			,i.is_hypothetical
			,iif(i.fill_factor = 0, 100, i.fill_factor)
			,i.allow_row_locks
			,i.allow_page_locks
			,i.optimize_for_sequential_key
			,i.has_filter
			,isnull(i.filter_definition,'''')
			,i.is_primary_key
			,i.is_unique_constraint
			,au.[rowcount]
			,au.[data_size]
			,i.data_space_id
			,ps.[partition scheme]
			,ps.[partition column]
		from
			sys.indexes as i
			inner join sys.objects as o
				on i.object_id = o.object_id
				and o.is_ms_shipped = 0
			cross apply (
				select
					 '','' + quotename(c.name) + iif(ic.is_descending_key = 0, '' asc'', '' desc'')
				from
					sys.index_columns as ic
					inner join sys.columns as c
						on c.object_id = ic.object_id
						and ic.column_id = c.column_id
				where
					ic.object_id = i.object_id
					and ic.index_id = i.index_id
					and ic.is_included_column = 0
				order by 
					ic.key_ordinal
				for xml path(''''),root(''x''),type
			) as ic([key])
			outer apply (
				select
					'','' + quotename(c.name)
				from
					sys.index_columns as ic
					inner join sys.columns as c
						on c.object_id = ic.object_id
						and ic.column_id = c.column_id
				where
					ic.object_id = i.object_id
					and ic.index_id = i.index_id
					and ic.is_included_column = 1
				order by 
					ic.key_ordinal
				for xml path(''''),root(''x''),type
			) as ic2(incl)
			outer apply (
				select
					ps.[name] as [partition scheme]
					,c.[name] as [partition column]
				from
					sys.tables as t
					inner join sys.index_columns as ic   
						on ic.[object_id] = i.[object_id]   
						and ic.index_id = i.index_id   
						and ic.partition_ordinal >= 1
					inner join sys.partition_schemes as ps   
						on ps.data_space_id = i.data_space_id   
					inner join sys.columns as c   
						on t.[object_id] = c.[object_id]   
						and ic.column_id = c.column_id   
				where t.[object_id] = i.[object_id]   
			) as ps
			left join (
				select
					p.object_id
					,p.index_id
					,sum(p.[rows]) as [rowcount]
					,sum(a.data_pages) * 8 as [data_size]
				from
					sys.partitions as p
					inner join sys.allocation_units as a
						on p.partition_id = a.container_id
				group by
					p.object_id
					,p.index_id
			) as au
				on au.object_id = i.object_id
				and au.index_id = i.index_id
		' + iif(@exclude_indexes <> '', 'where not (' + @exclude_indexes + ')', '')

	insert into [#SYS_indexes] ([purpose],[server],[db],[table id],[name],[key column names],[included column names],[index type],[ignore dup key],[is unique],[is padded],[is disabled],[is hypothetical],[fill factor],[allow row locks],[allow page locks],[optimize for sequential key],[has filter],[filter definition],[is primary key],[is unique constraint],[rowcount],[data_size],[data space id],[partition scheme],[partition column])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_indexes] ([purpose],[server],[db],[table id],[name],[key column names],[included column names],[index type],[ignore dup key],[is unique],[is padded],[is disabled],[is hypothetical],[fill factor],[allow row locks],[allow page locks],[optimize for sequential key],[has filter],[filter definition],[is primary key],[is unique constraint],[rowcount],[data_size],[data space id],[partition scheme],[partition column])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
		
	--ограничения
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,df.parent_object_id
			,quotename(df.[name])
			,quotename(c.name)
			,df.[type]
			,df.[definition]
			,''default '' + df.[definition] + '' for '' + quotename(c.name)
		from
			sys.default_constraints as df
			inner join sys.objects as o
				on o.object_id = df.parent_object_id
			inner join sys.columns as c
				on c.object_id = df.parent_object_id
				and c.column_id = df.parent_column_id
		where
			o.is_ms_shipped <> 1
			' + iif(@exclude_defaults <> '', 'and not (' + @exclude_defaults + ')', '') + '
		
		select
			@purpose
			,@server
			,db_name()
			,ch.parent_object_id
			,quotename(ch.[name])
			,''''
			,ch.[type]
			,ch.[definition]
			,''check '' + ch.[definition]
		from sys.check_constraints as ch
			
		select
			@purpose
			,@server
			,db_name()
			,fk.parent_object_id
			,quotename(fk.[name])
			,stuff(parent_fkc.[parent colunms].value(''/x[1]'',''nvarchar(max)''),1,1,'''')
			,fk.[type]
			,''''
			,'' foreign key ('' + stuff(parent_fkc.[parent colunms].value(''/x[1]'',''nvarchar(max)''),1,1,'''') + '') references '' + quotename(s.name) + ''.'' + quotename(t.name) collate Cyrillic_General_100_CI_AS
				+ '' ('' + stuff(fkc.[columns].value(''/x[1]'',''nvarchar(max)''),1,1,'''') + '') on update '' + lower(replace(fk.update_referential_action_desc,''_'','' '')) + '' on delete '' + lower(replace(fk.delete_referential_action_desc,''_'','' ''))
		from
			sys.foreign_keys as fk
			inner join sys.tables as t
				on t.object_id = fk.referenced_object_id
			inner join sys.schemas as s
				on s.schema_id = t.schema_id
			cross apply (
				select
					'','' + quotename(c.name)
				from
					sys.foreign_key_columns as fkc
					inner join sys.columns as c
						on c.object_id = fk.parent_object_id
						and c.column_id = fkc.parent_column_id
				where fkc.constraint_object_id = fk.object_id
				order by fkc.constraint_column_id
				for xml path(''''),root(''x''),type
			) parent_fkc([parent colunms])
			cross apply (
				select
					'','' + quotename(c.name)
				from
					sys.foreign_key_columns as fkc
					inner join sys.columns as c
						on c.object_id = fk.referenced_object_id
						and c.column_id = fkc.referenced_column_id
				where fkc.constraint_object_id = fk.object_id
				order by fkc.constraint_column_id
				for xml path(''''),root(''x''),type
			) fkc([columns])
	'
	
	insert into [#SYS_constraints] ([purpose],[server],[db],[table id],[name],[column names],[type],[definition],[definition_full])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_constraints] ([purpose],[server],[db],[table id],[name],[column names],[type],[definition],[definition_full])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
	
	--модули
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,o.object_id
			,quotename(s.name)
			,quotename(o.name)
			,o.type
			,isnull(t.is_disabled, 0) --objectproperty(o.object_id,''ExecIsTriggerDisabled'')
			,0 --objectproperty(o.object_id,''IsEncrypted'')
			,isnull(sm.is_schema_bound, 0) --objectproperty(o.object_id,''IsSchemaBound'')
			,sm.[definition] --object_definition(o.object_id)
			,lower(iif(charindex(''_'',o.type_desc) = 0, o.type_desc, right(o.type_desc, charindex(''_'',reverse(o.type_desc))-1)))
			,o.parent_object_id
			,quotename(s2.name)
			,quotename(o2.name)
		from
			sys.objects as o
			inner join sys.schemas as s
				on s.schema_id = o.schema_id
			left join sys.triggers as t
				on o.object_id = t.object_id
			left join sys.sql_modules as sm
				on sm.object_id = o.object_id
			left join sys.objects as o2
				on o.parent_object_id = o2.object_id
			left join sys.schemas as s2
				on s2.schema_id = o2.schema_id
		where
			o.type in (''FN'',''IF'',''P'',''V'',''TF'',''TR'')
			and o.is_ms_shipped = 0
		' + iif(@exclude_modules <> '', 'and not (' + @exclude_modules + ')', '')
			
	insert into [#SYS_modules] ([purpose],[server],[db],[id],[schema],[name],[object type],[is trg disabled],[is encrypted],[is binding],[definition],[type desc],[parent id],[parent schema],[parent name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_modules] ([purpose],[server],[db],[id],[schema],[name],[object type],[is trg disabled],[is encrypted],[is binding],[definition],[type desc],[parent id],[parent schema],[parent name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
	
	--синонимы
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,s.object_id
			,quotename(s2.name)
			,quotename(s.name)
			,s.base_object_name
		from
			sys.synonyms as s
			inner join sys.schemas as s2
				on s.schema_id = s2.schema_id
		'
		
	insert into [#SYS_synonyms] ([purpose],[server],[db],[id],[schema],[name],[base object name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	
	insert into [#SYS_synonyms] ([purpose],[server],[db],[id],[schema],[name],[base object name])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
	
	--последовательности
	set @query = '
		use ' + quotename(@db) + '
		set tran isolation level read uncommitted
		select
			@purpose
			,@server
			,db_name()
			,s.object_id
			,quotename(s2.name)
			,quotename(s.name)
			,iif(t.[name] in (''decimal'',''numeric''), concat(t.[name],''('',s.precision,'')''),t.[name])
			,cast(s.start_value as bigint)
			,cast(s.increment as bigint)
			,cast(s.minimum_value as bigint)
			,cast(s.maximum_value as bigint)
			,s.is_cycling
			,s.is_cached
			,s.cache_size
			,cast(s.last_used_value as bigint)
		from
			sys.sequences as s
			inner join sys.types as t
				on t.user_type_id = s.user_type_id
			inner join sys.schemas as s2
				on s.schema_id = s2.schema_id
	'
	
	insert into [#SYS_sequences] ([purpose],[server],[db],[id],[schema],[name],[type],[start value],increment,[minimum value],[maximum value],[is cycling],[is cached],[cache size],[last used value])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @@servername,@purpose = @t_target
	
	set @query = replace(@query, 'sys.', @metadata_db + '..SRC_')
	
	insert into [#SYS_sequences] ([purpose],[server],[db],[id],[schema],[name],[type],[start value],increment,[minimum value],[maximum value],[is cycling],[is cached],[cache size],[last used value])
	exec sp_executesql
			@query
			,N'@server nvarchar(50), @purpose nvarchar(10)'
			,@server = @source_server, @purpose = @t_source
	
	end

	begin--сводная таблица

	drop table if exists [#PIVOT_TABLE]

	create table [#PIVOT_TABLE] (
		[object desc] nvarchar(128)
		,[parent object desc] nvarchar(128)
		,[source server] nvarchar(128)
		,[source db] nvarchar(128)
		,[source parent object] nvarchar(128)
		,[source object] nvarchar(128)
		,[target server] nvarchar(128)
		,[target db] nvarchar(128)
		,[target parent object] nvarchar(128)
		,[target object] nvarchar(128)
		,[sql text] nvarchar(max)
		,[action] nvarchar(10) index ix_act nonclustered
		,[order] int
		,[info] nvarchar(128)
	)

	--схемы
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_schema
		,null
		,s.[server]
		,s.db
		,null
		,s.[name]
		,t.[server]
		,t.db
		,null
		,t.[name]
		,case
			when s.[name] is null
				then 'drop schema ' + t.[name]
			when t.[name] is null
				then 'create schema ' + s.[name]
			else '' end
	from
		[#SYS_schemas] as s
		full join [#SYS_schemas] as t
			on s.purpose <> t.purpose
			and s.[name] = t.[name]
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
		
	--функции секционирования
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_partition_function
		,null
		,s.[server]
		,s.db
		,''
		,s.[name]
		,t.[server]
		,t.db
		,''
		,t.[name]
		,case
			when s.[name] is null
				then 'drop partition function ' + quotename(t.[name])
			when t.[name] is null
				then s.[definition]
			else '' end
	from
		[#SYS_partition_functions] as s
		full join [#SYS_partition_functions] as t
			on s.purpose <> t.purpose
			and t.[definition checksum] = s.[definition checksum]
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)

	insert into [#SYS_partition_alter] ([type], [name])
	select distinct
		@t_partition_function
		,isnull(pt.[source object],pt.[target object])
	from [#PIVOT_TABLE] as pt
	where
		pt.[object desc] = @t_partition_function
		and pt.[sql text] <> ''
	option (maxdop 8)

	--схемы секционирования
	;with [partition_schemes] as (
		select
			ps.*
			,iif(pa.[name] is not null, 1, 0) as [re-create]
		from
			[#SYS_partition_schemes] as ps
			inner join [#SYS_partition_functions] as pf
				on ps.[function id] = pf.id
				and pf.purpose = ps.purpose
			left join [#SYS_partition_alter] as pa
				on pa.[name] = pf.[name]
				and pa.[type] = @t_partition_function
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_partition_scheme
		,null
		,s.[server]
		,s.db
		,''
		,s.[name]
		,t.[server]
		,t.db
		,''
		,t.[name]
		,case
			when s.[name] is null
				then 'drop partition scheme ' + quotename(t.[name])
			when t.[name] is null
				then s.[definition]
			else '' end
	from
		[partition_schemes] as s
		full join [partition_schemes] as t
			on s.purpose <> t.purpose
			and t.[definition checksum] = s.[definition checksum]
			and (t.[re-create] | s.[re-create] = 0)
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)

	insert into [#SYS_partition_alter] ([type], [name])
	select distinct
		@t_partition_scheme
		,isnull(pt.[source object],pt.[target object])
	from [#PIVOT_TABLE] as pt
	where
		pt.[object desc] = @t_partition_scheme
		and pt.[sql text] <> ''
	option (maxdop 8)

	--таблицы
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_table
		,null
		,s.[server]
		,s.db
		,null
		,s.[schema.table]
		,t.[server]
		,t.db
		,null
		,t.[schema.table]
		,case
			when t.[schema.table] is null
				then 'create table ' + s.[schema.table] + ' ( ' + stuff((
					select
						',' + c.[name] + ' ' + case
							when c.[is computed] = 1
								then ' as ' + c.[definition for computed] + iif(c.[is persisted] = 1,' persisted','')
							when c.[is identity] = 1
								then c.[type name] + c.[type size] + concat(' identity (',c.[seed value],',',c.[increment value],')')
							else
								c.[type name] + c.[type size] + ' ' + isnull(' collate ' + c.collation,'') + iif(c.[is nullable] = 0,' not','') + ' null'
							end + @c_nl
					from [#SYS_columns] as c
					where
						c.[purpose.table id] = s.[purpose.id]
					order by c.[ordinal position]
					for xml path(''),root('x'),type
				).value('/x[1]','nvarchar(max)'),1,1,'') + ' )'
			when s.[schema.table] is null
				then 'drop table ' + t.[schema.table]
			when s.[lock escalation] <> t.[lock escalation]
				then 'alter table ' + t.[schema.table] + ' set (lock_escalation = ' + s.[lock escalation] + ')'
			else '' end
	from
		( [#SYS_tables] as s
		full join [#SYS_tables] as t
			on t.purpose <> s.purpose
			and t.[schema.table] = s.[schema.table] )
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
	
	--колонки
	;with [columns] as (
		select
			c.*
			,t.[schema.table]
			,concat(c.[type name],c.[type size],c.[is nullable]) as [type check]
			,checksum(upper(replace(replace(replace(replace(c.[definition for computed],char(32),''),char(9),''),char(10),''),char(13),''))) as [computed check]
		from
			[#SYS_tables] as t
			inner join [#SYS_tables] as t2
				on t.purpose <> t2.purpose
				and t2.[schema.table] = t.[schema.table]
			inner join [#SYS_columns] as c
				on c.[purpose.table id] = t.[purpose.id]
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text], [order])
	select
		@t_column
		,@t_table
		,s.[server]
		,s.db
		,s.[schema.table]
		,s.[name]
		,t.[server]
		,t.db
		,t.[schema.table]
		,t.[name]
		,case
			when s.[name] is null
				then 'alter table ' + t.[schema.table] + ' drop column ' + t.[name]
			when t.[name] is null
				then 'alter table ' + s.[schema.table] + ' add ' + s.[name] + ' ' + case
							when s.[is computed] = 1
								then ' as ' + s.[definition for computed] + iif(s.[is persisted] = 1,' persisted','')
							when s.[is identity] = 1
								then s.[type name] + s.[type size] + concat(' identity (',s.[seed value],',',s.[increment value],')')
							else
								s.[type name] + s.[type size] + isnull(' collate ' + s.collation,'') + case
									when s.[is nullable] = 1
										then ' null'
									when s.[is nullable] = 0 and s.[name] <> '[timestamp]'
										then
											' null ' + @c_nl
											+ 'alter table ' + s.[schema.table] + ' disable trigger all' + @c_nl
											+ 'exec ('' update ' + replace(s.[schema.table] + ' set ' + s.[name] + ' = ' + isnull(df.[definition],s.[default for type]),'''','''''') + ''')' + @c_nl
											+ 'alter table ' + s.[schema.table] + ' alter column ' + s.[name] + ' ' + s.[type name] + s.[type size] + ' not null' +@c_nl
											+ 'alter table ' + s.[schema.table] + ' enable trigger all'
									else iif(s.[is nullable] = 0,' not','') + ' null' end end
			when (s.[is identity] = 0 and t.[is identity] = 1) or (s.[is computed] = 0 and t.[is computed] = 1)
				then
					'alter table ' + t.[schema.table] + ' add [$temp_i_clmn] ' + s.[type name] + s.[type size] + isnull(' collate ' + s.collation,'') + ' null' + @c_nl
					+ 'alter table ' + s.[schema.table] + ' disable trigger all' + @c_nl
					+ 'exec ('' update ' + t.[schema.table] + ' set [$temp_i_clmn] = ' + t.[name] + ''')' + @c_nl
					+ 'alter table ' + t.[schema.table] + ' drop column ' + t.[name] + @c_nl
					+ 'exec sp_rename ''' + t.[schema.table] + '.[$temp_i_clmn]'',''' + parsename(s.[name],1) + ''', ''column''' + @c_nl
					+ iif(s.[is nullable] = 0, 'alter table ' + t.[schema.table] + ' alter column ' + t.[name] + ' not null ' + @c_nl,'')
					+ 'alter table ' + s.[schema.table] + ' enable trigger all'
			when t.[name] <> '[timestamp]' and s.[type check] <> t.[type check]
				then
					iif(s.[is nullable] = 0 and t.[is nullable] = 1,'update ' + t.[schema.table] + ' set ' + t.[name] + ' = ' + isnull(df.[definition],s.[default for type]) + ' where ' + t.[name] + ' is null' + @c_nl,'')
					+ 'alter table ' + t.[schema.table] + ' alter column ' + t.[name] + ' ' + s.[type name] + s.[type size] + ' ' + isnull(' collate ' + s.collation,'') + iif(s.[is nullable] = 0,' not','') + ' null'
			else '' end
		,isnull(s.[ordinal position], t.[ordinal position])
	from
		( [columns] as s
		full join [columns] as t
			on t.purpose <> s.purpose
			and t.[schema.table] = s.[schema.table]
			and t.[name] = s.[name]
			and not (s.[is computed] = 1 and t.[is computed] = 0)
			and not (s.[is identity] = 1 and t.[is identity] = 0)
			and isnull(t.[increment value],0) = isnull(s.[increment value],0)
			and t.[computed check] = s.[computed check] )
		outer apply (
			select c.[definition]
			from [#SYS_constraints] as c
			where
				c.[purpose.table id] = s.[purpose.table id]
				and c.purpose = @t_source
				and c.[column names] = s.[name]
				and c.[type] = 'D') as df
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	
	insert into [#SYS_columns_alter] ([schema.table], [name])
	select distinct
		isnull(pt.[source parent object],pt.[target parent object])
		,isnull(pt.[source object],pt.[target object])
	from [#PIVOT_TABLE] as pt
	where
		pt.[object desc] = @t_column
		and pt.[sql text] <> ''
	option (maxdop 8)
	
	--индексы
	;with [indexes] as (
		select
			i.*
			,t.[schema.table]
			,upper(concat(i.[key column names], i.[included column names])) as [column check]
			,concat(i.[allow row locks],i.[allow page locks],i.[optimize for sequential key],i.[ignore dup key]) as [options check]
			,checksum(upper(replace(replace(replace(replace(i.[filter definition],char(32),''),char(9),''),char(10),''),char(13),''))) as [filter check]
			,iif(ca.[count] > 0, 1, 0) | iif(pa.[name] is not null, 1, 0) as [re-create]
		from
			[#SYS_indexes] as i
			inner join (
				select
					[schema.table]
					,[purpose.id]
				from [#SYS_tables] as t
				union all
				select
					[schema.object]
					,[purpose.id]
				from [#SYS_modules] as t
				where [object type] = 'V' 
			) as t
				on i.[purpose.table id] = t.[purpose.id]
			left join [#SYS_partition_alter] as pa
				on i.[partition scheme] = pa.[name]
				and pa.[type] = @t_partition_scheme
			outer apply (
				select count(*) as [count]
				from [#SYS_columns_alter] as ca
				where
					ca.[schema.table] = t.[schema.table]
					and (charindex(ca.[name],i.[key column names]) > 0 or charindex(ca.[name],i.[included column names]) > 0)
				) as ca
		where i.[index type] <> 'heap'
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text],[info])
	select
		@t_index
		,@t_table
		,s.[server]
		,s.db
		,s.[schema.table]
		,s.[name]
		,t.[server]
		,t.db
		,t.[schema.table]
		,t.[name]
		,case
			when t.[name] is null
				then
					iif(s.[is primary key] | s.[is unique constraint] = 1,
						'alter table ' + s.[schema.table] + ' add constraint ' + s.[name] + iif(s.[is primary key] = 1, ' primary key ', ' unique ') + s.[index type] + ' ( ' + s.[key column names] + ' ) ',
						'create ' + iif(s.[is unique] = 1,' unique ', '') + s.[index type] + ' index ' + s.[name] + ' on ' + s.[schema.table] + ' ( ' + s.[key column names] + ' ) '
					+ iif(s.[included column names] = '', '', concat(' include (',s.[included column names],') '))
					+ iif(s.[has filter] = 1,' where ' + s.[filter definition],''))
					+ ' with (
						fillfactor = ' + cast(s.[fill factor] as nvarchar(3))	+ '
						,allow_page_locks = ' + iif(s.[allow page locks] = 1, 'on', 'off') + '
						,allow_row_locks = ' + iif(s.[allow row locks] = 1, 'on', 'off') + '
						,ignore_dup_key = ' + iif(s.[ignore dup key] = 1, 'on', 'off') + '
						,optimize_for_sequential_key = ' + iif(s.[optimize for sequential key] = 1, 'on', 'off') + ' ) '
					+ ' on ' + iif(s.[partition scheme] is null, quotename(@target_filegroup), quotename(s.[partition scheme]) + '('+ quotename(s.[partition column]) + ')')
			when s.[schema.table] is null
				then
					iif(t.[is primary key] | t.[is unique constraint] = 1,
						'alter table ' + t.[schema.table] + ' drop constraint ' + t.[name] + @c_nl,
						'drop index ' + t.[name] + ' on ' + t.[schema.table])
			when t.[options check] <> s.[options check]
				then 'alter index ' + t.[name] + ' on ' + t.[schema.table] + ' set (
						optimize_for_sequential_key = ' + iif(s.[optimize for sequential key] = 1, 'on', 'off') +
						iif(s.[is primary key] | s.[is unique constraint] = 1, '', ',ignore_dup_key = ' + iif(s.[ignore dup key] = 1, 'on', 'off')) + '
						,allow_page_locks = ' + iif(s.[allow page locks] = 1, 'on', 'off') + '
						,allow_row_locks = ' + iif(s.[allow row locks] = 1, 'on', 'off') + ' )'
			else '' end
			+ iif(s.[is disabled] = 1 and t.[is disabled] = 0, @c_nl + 'alter index ' + s.[name] + ' on ' + s.[schema.table] + ' disable', '')
		,concat(isnull(s.[rowcount], t.[rowcount]), ' rows ≥', isnull(s.[data_size], t.[data_size]), ' KB')
	from
		[indexes] as s
		full join [indexes] as t
			on t.purpose <> s.purpose
			and t.[schema.table] = s.[schema.table]
			--and t.[name] = s.[name]
			and t.[column check] = s.[column check]
			and t.[is unique] = s.[is unique]
			and t.[index type] = s.[index type]
			--and t.[has filter] = s.[has filter]
			and t.[filter check] = s.[filter check]
			and (t.[re-create] | s.[re-create] = 0)
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
	
	--ограничения
	;with [constraints] as (
		select
			t.[schema.table]
			,c.*
			,iif(ca.[count] > 0, 1, 0) as [re-create]
		from
			[#SYS_tables] as t
			inner join [#SYS_constraints] as c
				on c.[purpose.table id] = t.[purpose.id]
			outer apply (
				select count(*) as [count]
				from [#SYS_columns_alter] as ca
				where
					ca.[schema.table] = t.[schema.table]
					and charindex(ca.[name],c.[column names]) > 0
				) as ca
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_constraint
		,@t_table
		,s.[server]
		,s.db
		,s.[schema.table]
		,s.[name]
		,t.[server]
		,t.db
		,t.[schema.table]
		,t.[name]
		,case
			when s.[name] is null
				then 'alter table ' + t.[schema.table] + ' drop constraint ' + t.[name]
			when t.[name] is null
				then 'alter table ' + s.[schema.table] + ' with nocheck add ' + s.[definition_full]
			else '' end
	from
		[constraints] as s
		full join [constraints] as t
			on t.purpose <> s.purpose
			and t.[schema.table] = s.[schema.table]
			and t.[definition_full] = s.[definition_full]
			and (t.[re-create] | s.[re-create] = 0)
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
	
	--модули
	;with [modules] as (
		select
			m.*
			,checksum(upper(replace(replace(replace(replace(m.[definition],char(32),''),char(9),''),char(10),''),char(13),''))) as [object check]
		from [#SYS_modules] as m
		where m.[is encrypted] = 0
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		isnull(s.[type desc], t.[type desc])
		,null
		,s.[server]
		,s.db
		,null
		,s.[schema.object]
		,t.[server]
		,t.db
		,null
		,t.[schema.object]
		,case
			when s.[schema.object] is not null and t.[schema.object] is null
				then s.[definition]
			when s.[schema.object] is null and t.[schema.object] is not null
				then 'drop ' + t.[type desc] + t.[schema.object]
			else '' end
			+ iif(s.[object type] = 'TR' and (t.[schema.object] is null or s.[is trg disabled] <> t.[is trg disabled])
				,iif(s.[is trg disabled] = 1
					,@marker + 'disable trigger ' + s.[schema.object] + ' on ' + s.[parent schema.object] + @marker
					,@marker + 'enable trigger ' + s.[schema.object] + ' on ' + s.[parent schema.object] + @marker)
				,'')
	from
		[modules] as s
		full join [modules] as t
			on t.purpose <> s.purpose
			and t.[object check] = s.[object check]
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
	
	--синонимы
	;with [synonyms] as (
		select s.*
		from [#SYS_synonyms] as s
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_synonym
		,null
		,s.[server]
		,s.db
		,null
		,s.[schema.object]
		,t.[server]
		,t.db
		,null
		,t.[schema.object]
		,case
			when s.[schema.object] is not null and t.[schema.object] is null
				then 'create synonym ' + s.[schema.object] + ' for ' + s.[base object name]
			when s.[schema.object] is null and t.[schema.object] is not null
				then 'drop synonym ' + t.[schema.object]
			else '' end
	from
		[synonyms] as s
		full join [synonyms] as t
			on t.purpose <> s.purpose
			and t.[schema.object] = s.[schema.object]
			and t.[base object name] = s.[base object name]
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	option (maxdop 8)
	
	--последовательности
	;with [sequences] as (
		select
			s.*
			,concat(s.[start value],s.increment,s.[minimum value],s.[maximum value],s.[is cycling],s.[is cached],s.[cache size]) as [options check]
		from [#SYS_sequences] as s
	)
	insert into [#PIVOT_TABLE] ([object desc],[parent object desc],[source server],[source db],[source parent object],[source object],[target server],[target db],[target parent object],[target object],[sql text])
	select
		@t_sequence
		,null
		,s.[server]
		,s.db
		,null
		,s.[schema.object]
		,t.[server]
		,t.db
		,null
		,t.[schema.object]
		,case
			when s.[schema.object] is not null and t.[schema.object] is null
				then concat('create sequence ',s.[schema.object],' as ',s.[type],' start with ',s.[start value],' increment by ',s.increment,' minvalue ',s.[minimum value],' maxvalue '
												,s.[maximum value],iif(s.[is cycling] = 1,'','no'),' cycle ',iif(s.[cache size] is null,' no cache',concat(' cache ',s.[cache size])))
			when s.[schema.object] is null and t.[schema.object] is not null
				then 'drop sequence ' + t.[schema.object]
			else '' end
	from
		[sequences] as s
		full join [sequences] as t
			on t.purpose <> s.purpose
			and t.[schema.object] = s.[schema.object]
			and s.[options check] = t.[options check]
	where
		(s.purpose = @t_source
			or t.purpose = @t_target)
	
	update [#PIVOT_TABLE] set
		[action] = case
			when [source object] is not null and [target object] is null and [sql text] <> ''
				then @t_create
			when [source object] is null and [target object] is not null and [sql text] <> ''
				then @t_drop
			when [source object] is not null and [target object] is not null and [sql text] <> ''
				then @t_alter
			else '' end
	end

	insert into [#SCRIPT_TABLE] (db, current_step, step_desc, sql_text, object_desc)
	select
		@db
		,row_number() over(order by
			case
				when pt.[object desc] in (@t_function, @t_procedure, @t_trigger) and pt.[action] = @t_drop then 1
				when pt.[object desc] = @t_constraint and pt.[action] = @t_drop then 2
				when pt.[object desc] = @t_index and pt.[action] = @t_drop then 3
				when pt.[object desc] = @t_view and pt.[action] = @t_drop then 4
				when pt.[object desc] = @t_column and pt.[action] = @t_drop then 5
				when pt.[object desc] = @t_table and pt.[action] = @t_drop then 6
				when pt.[object desc] = @t_partition_scheme and pt.[action] = @t_drop then 7
				when pt.[object desc] = @t_partition_function and pt.[action] = @t_drop then 8
				when pt.[object desc] = @t_schema and pt.[action] = @t_create then 9
				when pt.[object desc] = @t_partition_function and pt.[action] = @t_create then 10
				when pt.[object desc] = @t_partition_scheme and pt.[action] = @t_create then 11
				when pt.[object desc] in (@t_column, @t_table, @t_constraint) then 12
				when pt.[object desc] = @t_view and pt.[action] = @t_alter then 13
				when pt.[object desc] = @t_view and pt.[action] = @t_create then 14
				when pt.[object desc] = @t_index and pt.[action] = @t_alter then 15
				when pt.[object desc] = @t_index and pt.[action] = @t_create then 16
				when pt.[object desc] in (@t_function, @t_procedure, @t_trigger) and pt.[action] = @t_alter then 17
				when pt.[object desc] in (@t_function, @t_procedure, @t_trigger) and pt.[action] = @t_create then 18
				when pt.[object desc] = @t_synonym and pt.[action] = @t_drop then 19
				when pt.[object desc] = @t_synonym and pt.[action] = @t_create then 20
				when pt.[object desc] = @t_sequence and pt.[action] = @t_drop then 21
				when pt.[object desc] = @t_sequence and pt.[action] = @t_create then 22
				when pt.[object desc] = @t_schema and pt.[action] = @t_drop then 23
				else 100 end
			,isnull(isnull(pt.[source parent object],pt.[source object]),isnull(pt.[target parent object],pt.[target object]))
			,case
				when pt.[object desc] = @t_column and pt.[action] = @t_alter then 1
				when pt.[object desc] = @t_column and pt.[action] = @t_create then 2
				when pt.[object desc] = @t_table and pt.[action] = @t_alter then 3
				when pt.[object desc] = @t_table and pt.[action] = @t_create then 4
				when pt.[object desc] = @t_constraint and pt.[action] = @t_create then 5
				else 100 end
			,pt.[order]
		) as current_step
		,pt.[action] + ' ' + pt.[object desc] + ': ' + isnull(isnull(pt.[target parent object] + '.' + pt.[target object],pt.[target object]),isnull(pt.[source parent object] + '.' + pt.[source object],pt.[source object])) as step_desc
		,pt.[sql text] as sql_text
		,pt.[object desc]
	from [#PIVOT_TABLE] as pt
	where
		pt.[action] <> ''
		--ФИЛЬТР
	option (maxdop 8)

fetch next from cursDB into @source_server, @db, @target_filegroup
end
close cursDB
deallocate cursDB



if not exists (select * from #SCRIPT_TABLE)
	print 'НЕТ ОБЪЕКТОВ ДЛЯ ОБНОВЛЕНИЯ !'
else if @ops = 0
begin--только генерация скрипта

	exec ('
		create or alter proc #PrintingLongText
			@text nvarchar(max)
		as
		set nocount on
		declare
			@pos int
		
		set @text = replace(replace(@text, char(13), char(10)), char(10) + char(10), char(10))
		
		while (1=1)
		begin
		    set @pos = charindex(char(10), @text)
		
			if (@pos = 0)
			begin
				print @text
				break
			end
			else if (@pos > 4000)
			begin
			    set @text = stuff(@text, charindex('','', @text, 3900), 1, '','' + char(10))
				continue
			end
		
		    print left(@text, @pos - 1)
		    
			set @text = substring(@text, @pos + 1, len(@text))
		end
	')

	print '--СКРИПТ СОЗДАН: ' + format(getdate(), 'dd MMMM yyyy HH:mm')
	print 'set nocount on'
	print 'if left(@@servername, 12) <> ''' + left(@@servername, 12) + '''	throw 50000, ''СКРИПТ ПРЕДНАЗНАЧЕН ДЛЯ ДРУГОГО СЕРВЕРА !'', 1'

	select @message = string_agg('--' + upper(db) + '	 нет объектов для обновления', @c_nl) within group (order by l.db)
	from #db_list as l
	where
		not exists (select * from #SCRIPT_TABLE as s where l.db = s.db)
		and @@servername like '%' + l.trg_srv + '%'

	if @message is not null
		print @message

	print 'if object_id(''tempdb..#toApply'',''U'') is null select current_step, db, cast('''' as nvarchar(max)) as result into #toApply from (values (null,null,null)'
	print ''
	print '--↓↓↓ УДАЛИТЬ ИЛИ ЗАКОММЕНТИРОВАТЬ НЕНУЖНЫЕ СТРОКИ НИЖЕ ↓↓↓'

	declare cursRW cursor for
		select
			current_step
			,db
			,step_desc
		from #SCRIPT_TABLE
		order by
			db
			,object_desc
			,substring(step_desc, charindex(':', step_desc) + 1, len(step_desc))
			,current_step
	open cursRW
	fetch next from cursRW into @current_step, @db, @step_desc
	while @@fetch_status = 0
	begin
		set @step_desc = formatmessage('%-20s %s', trim(substring(@step_desc, 1, charindex(':', @step_desc))), trim(substring(@step_desc, charindex(':', @step_desc) + 1, len(@step_desc))))

		print formatmessage(',(%-4s,%-25s,%s)', @current_step, quotename(@db,''''), quotename(@step_desc,''''))

		fetch next from cursRW into @current_step, @db, @step_desc
	end
	close cursRW
	deallocate cursRW
	   
	print '--↑↑↑ УДАЛИТЬ ИЛИ ЗАКОММЕНТИРОВАТЬ НЕНУЖНЫЕ СТРОКИ ВЫШЕ ↑↑↑
	
		) as t(current_step, db, step_desc)
	else
	begin
		select * from #toApply where len([result]) > 5 order by db, current_step

		;throw 50000, ''СКРИПТ УЖЕ ЗАПУСКАЛСЯ, СГЕНЕРИРУЙТЕ НОВЫЙ !'', 1
	end

	declare
		@result int
		,@message nvarchar(max)

	exec @result = sys.sp_getapplock
	    @Resource = ''' + @applock_const + '''
	    ,@LockMode = ''exclusive''
	    ,@LockOwner = ''session''
		,@LockTimeout = 0
		,@DbPrincipal = ''' + @user + '''
	
	if @result < 0
	begin
	    select @message = string_agg(request_session_id, '','')
		from sys.dm_tran_locks (nolock)
		where
			resource_type = ''APPLICATION''
			and request_owner_type = ''SESSION''
			and resource_description like ''%' + @applock_const + '%''
	
		set @message = concat(''ЗАКРОЙТЕ СЕССИИ: '', @message, '' !'')
		
		;throw 50000, @message, 1
	end
	'
	
	declare cursPR cursor for
		select
			db
			,current_step
			,step_desc
			,sql_text
		from [#SCRIPT_TABLE]
		order by
			db
			,current_step
	
	open cursPR
	fetch next from cursPR into @db, @current_step, @step_desc, @sql_text
	
	while (@@fetch_status = 0)
	begin
		print '--ДЕЙСТВИЕ: ' + upper(@step_desc)
		print 'if exists (select 1 from #toApply where db = ''' + @db + ''' and current_step = ' + @current_step + ' and result = '''') '
		print 'begin'
		print 'use ' + quotename(@db)
		print 'begin try'
		print 'exec ('''

		if @step_desc like 'create trigger%'
		begin
			set @sql_text2 = replace(substring(@sql_text, charindex(@marker, @sql_text), charindex(@marker, @sql_text, charindex(@marker, @sql_text) +3) - charindex(@marker, @sql_text) + len(@marker)), @marker, '')
			set @sql_text = stuff(@sql_text, charindex(@marker, @sql_text), charindex(@marker, @sql_text, charindex(@marker, @sql_text) +3) - charindex(@marker, @sql_text) + len(@marker), '')
		end
		
		set @sql_text = replace(@sql_text,'''','''''')

		if len(@sql_text) < 4000
			print @sql_text
		else
			exec #PrintingLongText @sql_text

		print ''')'

		if @step_desc like 'create trigger%'
		begin
			print 'exec ('''
			print @sql_text2
			print ''')'
		end

		print 'update #toApply set [result] = ''OK'' where db = ''' + @db + ''' and current_step = ' + @current_step
		print 'end try'
		print 'begin catch'
		print 'update #toApply set [result] = error_message() where db = ''' + @db + ''' and current_step = ' + @current_step
		print 'end catch'
		print 'end' + @c_nl
	
		fetch next from cursPR into @db, @current_step, @step_desc, @sql_text
	end
	
	close cursPR
	deallocate cursPR
	
	print '
		if exists (select * from #toApply where len([result]) > 5)
			select * from #toApply where len([result]) > 5 order by db, current_step
		else
			select ''СКРИПТ ВЫПОЛНИЛСЯ БЕЗ ОШИБОК !''	'
	
end
else if @ops = 0
begin--выполнение синхронизации метаданных

	print 'СТАРТ ВЫПОЛНЕНИЯ !'

	set @query = '
		drop table if exists ' + @synch_result_tbl + '
		create table ' + @synch_result_tbl + ' (
			[db] sysname
			,[step] int
			,[desc] nvarchar(max)
			,[error] nvarchar(max)
			,[DTM] datetime default (getdate())
		)
	'
	exec(@query)

	declare curs cursor for
		select
			db
			,current_step
			,step_desc
			,sql_text
		from [#SCRIPT_TABLE]
		order by
			db
			,current_step
	
	open curs
	fetch next from curs into @db, @current_step, @step_desc, @sql_text
	
	while (@@fetch_status = 0)
	begin
		begin try

			if @step_desc like 'create trigger%'
			begin
				set @sql_text2 = replace(substring(@sql_text, charindex(@marker, @sql_text), charindex(@marker, @sql_text, charindex(@marker, @sql_text) +3) - charindex(@marker, @sql_text) + len(@marker)), @marker, '')
				set @sql_text = stuff(@sql_text, charindex(@marker, @sql_text), charindex(@marker, @sql_text, charindex(@marker, @sql_text) +3) - charindex(@marker, @sql_text) + len(@marker), '')
				set @sql_text2 = 'use ' + quotename(@db) + ' exec(''' + @sql_text2 + ''')'
			end
			
			set @sql_text = 'use ' + quotename(@db) + ' exec(''' + replace(@sql_text,'''','''''') + ''')'
			
			exec @sql_text
	
			if @step_desc like 'create trigger%'
				exec (@sql_text2)
			
			set @result = 0
		end try
		begin catch
			set @message = error_message()
			set @query = '
				insert into ' + @synch_result_tbl + ' ([db], [step], [desc], [error])
				values (''' + @db + ''',''' + @current_step + ''',''' + @step_desc + ''',''' + @message + ''')'
			exec (@query)
			set @result = 1
		end catch

		set @message = format(getdate(),'[HH:mm]') + ' [' + @current_step + '/' + cast(@@cursor_rows as varchar(10)) + '] ' + iif(@result = 0, '[OK] ', '[ERROR] ') + @step_desc
		raiserror (@message, 10, 1) with nowait

		fetch next from curs into @db, @current_step, @step_desc, @sql_text
	end
	
	close curs
	deallocate curs
	
	set @query = '
		if exists (select * from ' + @synch_result_tbl + ')
		begin
			print ''ВЫПОЛНЕНИЕ ЗАВЕРШИЛОСЬ С ОШИБКАМИ !''
			select
				[db] as [База данных]
				,[step] as [Номер шага]
				,[desc] as [Описание]
				,[error] as [Текст ошибки]
			from ' + @synch_result_tbl + '
		end
		else
			print ''ВЫПОЛНЕНИЕ ЗАВЕРШИЛОСЬ БЕЗ ОШИБОК !''
	'
	exec (@query)

end
