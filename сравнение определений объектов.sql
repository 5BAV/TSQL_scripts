create or alter procedure [dbo].[Comparison_of_definitions]
	@left_srv varchar(30) -- левый сервер
	,@right_srv varchar(30) -- правый сервер
	,@left_db varchar(30) -- левая база
	,@right_db varchar(30) -- правая база
	,@types varchar(30) -- KI=индексы, остальные типы это колонка type из sys.objects
	,@show_defin bit = 0 -- итоговый вывод с определения
	,@only_different bit = 0 -- в итоговом выводе только разчающиеся
as
set nocount on

declare
	@query nvarchar(max)
	,@db varchar(30)
	,@link varchar(30)
	,@l_srv_name varchar(30)
	,@r_srv_name varchar(30)
	,@srv_name varchar(30)

-- таблица маппинга линков и серверов
select @l_srv_name = [name]
from 
where [data_source] = @left_srv

select @r_srv_name = [name]
from 
where [data_source] = @right_srv


if (@l_srv_name = @r_srv_name and @right_db = @left_db)
	throw 50000, 'Одинаковые имена серверов и баз данных', 1

drop table if exists #R_result
drop table if exists #L_result

create table #R_result	(
	srv varchar(30)
	,db varchar(30)
	,shm varchar(30)
	,obj varchar(200)
	,tp varchar(30)
	,crdt datetime
	,mfdt datetime
	,def varchar(max)
	,chksum as checksum(upper(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(replace(replace( SUBSTRING (def, CHARINDEX (obj, def), LEN(def)),char(32),''),char(9),''),char(10),''),char(13),''),'dbo',''),'].[',''),'[',''),']',''))) persisted
	,flnm as upper(concat(shm,'.',obj)) persisted
)
create table #L_result (
	srv varchar(30)
	,db varchar(30)
	,shm varchar(30)
	,obj varchar(200)
	,tp varchar(30)
	,crdt datetime
	,mfdt datetime
	,def varchar(max)
	,chksum as checksum(upper(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(replace(replace(replace( SUBSTRING (def, CHARINDEX (obj, def), LEN(def)),char(32),''),char(9),''),char(10),''),char(13),''),'dbo',''),'].[',''),'[',''),']',''))) persisted
	,flnm as upper(concat(shm,'.',obj)) persisted
)

select
	@db = @right_db
	,@link = @right_srv
	,@srv_name = @r_srv_name

begin try

while (1=1)
begin
	set @query = '
		use '+quotename(@db)+'

		' + iif(charindex('KI', @types) = 0, '', '
		select
			s.[schema name] as sch_name
			,s.[table name] + ''''.'''' + s.[index name] as obj_name
			,iif(s.[is primary key] | s.[is unique constraint] = 1,
					''''alter table '''' + s.[schema name] + ''''.'''' + s.[table name] + '''' add constraint '''' + s.[index name] + iif(s.[is primary key] = 1, '''' primary key '''', '''' unique '''') + s.[index type] + '''' ( '''' + s.[key column names] + '''' ) '''',
					''''create '''' + iif(s.[is unique] = 1,'''' unique '''', '''''''') + s.[index type] + '''' index '''' + s.[index name] + '''' on '''' + s.[schema name] + ''''.'''' + s.[table name] + '''' ( '''' + s.[key column names] + '''' ) ''''
				+ iif(s.[included column names] = '''''''', '''''''', concat('''' include ('''',s.[included column names],'''') ''''))
				+ iif(s.[has filter] = 1,'''' where '''' + s.[filter definition],'''''''')) as obj_def
			,create_date
			,modify_date
			,cast(case when s.[is primary key] = 1 then ''''PK'''' when s.[is unique constraint] = 1 then ''''UQ'''' else ''''IDX'''' end as char(2)) collate Latin1_General_CI_AS_KS_WS as obj_type
		into #ki
		from (
			select
				quotename(o.[name]) as [table name]
				,quotename(s.[name]) as [schema name]
				,quotename(i.[name]) as [index name]
				,stuff(ic.[key].value(''''/x[1]'''',''''nvarchar(max)''''),1,1,'''''''') collate Latin1_General_CI_AS_KS_WS as [key column names]
				,isnull(stuff(ic2.[incl].value(''''/x[1]'''',''''nvarchar(max)''''),1,1,''''''''),'''''''') collate Latin1_General_CI_AS_KS_WS as [included column names]
				,lower(i.[type_desc]) as [index type]
				,i.is_unique as [is unique]
				,i.has_filter as [has filter]
				,isnull(i.filter_definition,'''''''') as [filter definition]
				,i.is_primary_key as [is primary key]
				,i.is_unique_constraint as [is unique constraint]
				,o.create_date
				,o.modify_date
			from
				sys.indexes as i (nolock)
				inner join sys.objects as o (nolock)
					on i.object_id = o.object_id
					and o.is_ms_shipped = 0
				inner join sys.schemas as s (nolock)
					on s.schema_id = o.schema_id
				cross apply (
					select '''','''' + quotename(c.[name]) + iif(ic.is_descending_key = 0, '''' asc'''', '''' desc'''')
					from
						sys.index_columns as ic (nolock)
						inner join sys.columns as c (nolock)
							on c.object_id = ic.object_id
							and ic.column_id = c.column_id
					where
						ic.object_id = i.object_id
						and ic.index_id = i.index_id
						and ic.is_included_column = 0
					order by ic.key_ordinal
					for xml path(''''''''),root(''''x''''),type
				) ic([key])
				outer apply (
					select '''','''' + quotename(c.[name])
					from
						sys.index_columns as ic (nolock)
						inner join sys.columns as c (nolock)
							on c.object_id = ic.object_id
							and ic.column_id = c.column_id
					where
						ic.object_id = i.object_id
						and ic.index_id = i.index_id
						and ic.is_included_column = 1
					order by ic.key_ordinal
					for xml path(''''''''),root(''''x''''),type
				) ic2(incl)
			where i.[type_desc] <> ''''heap''''
		) as s') + '

		;with s as (
			select
				s.name as sch_name
				,o.name as obj_name
				,o.type
				,o.create_date
				,o.modify_date
				,object_definition(o.object_id) as obj_def
			from
				sys.objects as o (nolock)
				inner join sys.schemas as s (nolock)
					on s.schema_id = o.schema_id
				inner join string_split('''''+replace(@types,',','')+''''','''';'''') as ss
					on o.type = ss.value
		' + iif(charindex('KI', @types) = 0, '', '
			union all
			select
				sch_name
				,obj_name
				,obj_type
				,create_date
				,modify_date
				,obj_def
			from #ki
		')+ ')
		select
			'''''+@srv_name+'''''
			,'''''+@db+'''''
			,sch_name
			,obj_name
			,type
			,create_date
			,modify_date
			,rtrim(ltrim(isnull(obj_def,''''НЕТ ОПРЕДЕЛЕНИЯ (возможно проблема с правами или линком)''''))) as def
		from s
		'

	set @query = 'exec ('''+@query+''') at '+quotename(@link)

	if (@link = @right_srv)
		insert into #R_result (srv,db,shm,obj,tp,crdt,mfdt,def)
		exec @query
	else
	begin
		insert into #L_result (srv,db,shm,obj,tp,crdt,mfdt,def)
		exec @query

		break
	end

	select @db = @left_db, @link = @left_srv, @srv_name = @l_srv_name
end
end try
begin catch
	select error_message()
	throw;

end catch

;with cte as (
	select
		format(t.mfdt,'dd.MM.yyyy HH:mm') as ldtmf
		,t.obj as lobj
		,isnull(t.tp,t2.tp) as tp
		,isnull(t.shm,t2.shm) as shm
		,t2.obj as robj
		,format(t2.mfdt,'dd.MM.yyyy HH:mm') as rdtmf
		,iif(@show_defin=0,'', case
								when t.flnm = t2.flnm and t.tp = t2.tp and t.chksum = t2.chksum
								then t.def
								when t.flnm = t2.flnm and t.tp = t2.tp and t.chksum <> t2.chksum
								then concat('ЛЕВОЕ:',char(13)+char(10),t.def,char(10)+char(13),'ПРАВОЕ:',char(13)+char(10),t2.def)
								else isnull(t.def,t2.def) end) as def
		,t.def as l_def
		,t2.def as r_def
		,case when t.flnm = t2.flnm and t.tp = t2.tp and t.chksum = t2.chksum then 1
				when t.flnm = t2.flnm and t.tp = t2.tp and t.chksum <> t2.chksum then 2
				else 3 end as dif
	from
		#L_result as t
		full join #R_result as t2
			on t.flnm = t2.flnm
			and t.tp = t2.tp
			--and t.chksum = t2.chksum
)
select *
from cte
where (@only_different = 0 or dif > 1)
order by row_number() over(order by (case when lobj is not null and robj is not null then 1
									when lobj is not null and robj is null then 2
									else 3 end), tp, shm, lobj, robj)


go

