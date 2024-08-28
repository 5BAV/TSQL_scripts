;with ten as (select * from (values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) as t(n))
,ord as (select row_number() over(order by (select null)) as N from ten as t1, ten as t2, ten as t3, ten as t4) --10000
select
	o.[char]
	,count(*) as [count]
from
	[any table] as t with(nolock)
	cross apply (
		select
			substring(t.[any field], o.N, 1) as [char]
			,o.N
		from ord as o
		order by o.N
		offset 0 rows fetch next len(t.[any field]) rows only
	) as o
where o.[char] not in ('0','1','2','3','4','5','6','7','8','9', char(32))
group by o.[char]
order by 2, 1
option (maxdop 8)