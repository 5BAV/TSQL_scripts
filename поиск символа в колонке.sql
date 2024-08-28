declare
	@table sysname = 'dbo.[any table]'
	,@column sysname = '[any field]'
	,@symbols sysname = 'char(10), char(13), char(9)'
	
declare	@query varchar(max) = '
	;with ten as (select * from (values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) as t(n))
	,ord as (select row_number() over(order by (select null)) as N from ten as t1, ten as t2, ten as t3) --1000
	select 
		t.' + @column + '
		,string_agg(concat(unicode(o.[char]), ''['', o.N, '']''), '', '') within group (order by o.N) as [unicode (position)]
	from
		' + @table + ' as t with(nolock)
		cross apply (
			select top(len(t.' + @column + '))
				substring(t.' + @column + ', o.n, 1) as [char]
				,o.N
			from ord as o
			order by o.N
		) as o
	where o.[char] in (' + @symbols + ')
	group by t.' + @column + '
	order by t.' + @column + '
	option (maxdop 8)
'
exec (@query)

