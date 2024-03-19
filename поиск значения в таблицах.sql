
set nocount on

declare
	@srch_like varchar(100) = '%search%' -- текст для поиска
	,@col_like varchar(max) = '%column%' -- пусто или перечень колонок в которых искать 
	,@tbl_like varchar(max) = '%table%' -- пусто или перечень таблиц в которых искать
	,@row_cnt_less int = 150 -- искать в таблицах в которых количество строк <= значению


select
	@srch_like = isnull(ltrim(rtrim(@srch_like)), '')
	,@col_like = isnull(ltrim(rtrim(@col_like)), '')
	,@tbl_like = isnull(ltrim(rtrim(@tbl_like)), '')

if (ltrim(@srch_like) = '')
	throw 50000, '@srch_like не задан!', 1

drop table if exists #t1

select
	identity(int) as id
	,quotename(schema_name(t.schema_id)) + '.' + quotename(t.[name]) as Таблица
	,p.[rows] as Всего_строк
	,quotename(c.[name]) as Колонка
	--,tp.[name] as Тип
	,0 as Найдено
into #t1
from
	sys.tables as t (nolock)
	inner join	sys.indexes as i (nolock)
		on i.object_id = t.object_id
	inner join sys.columns as c (nolock)
		on c.object_id = t.object_id
	inner join sys.types as tp
		on c.user_type_id = tp.user_type_id
	inner join (
		select
			object_id
			,index_id
			,sum([rows]) as [rows]
		from sys.partitions (nolock)
		group by
			object_id
			,index_id
		having (((@row_cnt_less > 0) and (sum([rows]) between 1 and @row_cnt_less)) or (@row_cnt_less < 1))
	) as p
		on p.object_id = i.object_id
		and p.index_id = i.index_id
where 1=1
	and t.is_ms_shipped = 0
	and i.[type] in (0,1)
	and ((@tbl_like <> '' and t.[name] like @tbl_like) or (@tbl_like = ''))
	and ((@col_like <> '' and c.[name] like @col_like) or (@col_like = ''))
	and tp.[name] in ('text', 'ntext', 'varchar', 'char', 'nvarchar', 'nchar', 'sysname')
order by t.[name]
option (maxdop 8)	

declare
	@id int
	,@cnt int = 0
	,@query nvarchar(max) = ''
	,@max_id int = (select max(id) from #t1)

declare curs cursor for
	select
		id
		,'select @cnt = count(*) from ' + Таблица + ' (nolock) where ' + Колонка + ' like ''' + @srch_like + ''''
	from #t1
	order by id
open curs
fetch next from curs into @id, @query
while @@fetch_status = 0
begin
	
	exec sys.sp_executesql
		@query
		,N'@cnt int out'
		,@cnt = @cnt out

	update #t1 set
		Найдено = @cnt
	where id = @id

	raiserror('[%d/%d]', 10, 1, @id, @max_id) with nowait

fetch next from curs into @id, @query
end
close curs
deallocate curs

select
	db_name() as База
	,Таблица
	,Всего_строк
	,Колонка
	,Найдено
	--,'select ' + Колонка + ' from ' + Таблица + ' (nolock) where ' + Колонка + ' like ''' + @srch_like + '''' as Запрос
from #t1
where Найдено > 0
order by 1,3


