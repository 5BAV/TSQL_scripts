--1 ВАРИАНТ числа в одной системе
--указать два числа в одной системе и базу системы (от 2 до 36)

declare
	@number1 varchar(100) = 'f1f'
	,@number2 varchar(100) = '1f1'
	,@base int = 16

set nocount on

declare
	@array varchar(100) = '0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z'
	,@separator varchar(1) = ','
	,@dec bigint
	,@out varchar(100)

if len(@number2) > len(@number1)
begin
	set @out = @number1
	set @number1 = @number2
	set @number2 = @out
	set @out = null
end

declare @digits table (num int primary key)

;with ten as (
	select n
	from (values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) as t(n)
),cte as (
	select row_number() over(order by (select null)) as n
	from
		ten as t1
		cross join ten as t2
		cross join ten as t3
)
insert into @digits (num)
select n from cte

declare @dict table (
	ord int
	,val varchar(1) primary key
)

insert into @dict (ord, val)
select top (@base)
	row_number() over(order by (select null)) - 1
	,cast(s.[value] as varchar(1))
from string_split(@array, @separator) as s

;with c1 as (
	select top (len(@number1))
		t.num
		,substring(@number1, row_number() over(order by t.num), 1) as symb
	from @digits as t
	order by t.num
), c2 as (
	select top (len(@number2))
		t.num
		,substring(@number2, row_number() over(order by t.num), 1) as symb
	from @digits as t
	order by t.num
), t1 as (
	select
		t.symb
		,row_number() over(order by t.num desc) as ord
		,d.ord as val
	from
		c1 as t
		inner join @dict as d
			on d.val = t.symb
), t2 as (
	select
		t.symb
		,row_number() over(order by t.num desc) as ord
		,d.ord as val
	from
		c2 as t
		inner join @dict as d
			on d.val = t.symb

), s as (
select
    isnull(t1.val, 0) as val1
    ,isnull(t2.val, 0) as val2
	,t1.ord
from
	t1
	full join t2
		on t1.ord = t2.ord
union all
select
	0
	,0
	,(select max(ord) + 1 from t1)
), r as (
	select
		s.ord
		,s.val1
		,s.val2
		,(s.val1 + s.val2) / @base as Tnf
		,((s.val1 + s.val2) % @base) as newVal
	from s
	where s.ord = 1
	union all
	select
		s.ord
		,s.val1
		,s.val2
		,((s.val1 + s.val2 + r.Tnf) / @base) as Tnf
		,((s.val1 + s.val2 + r.Tnf) % @base) as newVal
	from
		s
		inner join r
			on r.ord + 1 = s.ord
), w as (
	select
		r.ord
		,d.val
	from
		r
		inner join @dict as d
			on d.ord = r.newVal
)
select @out = string_agg(val, '') within group (order by ord desc)
from w

select @out

go

--2 ВАРИАНТ число в любой системе, а инкремент в десятичной

declare
	@number varchar(100) = 'fff' --число в системе база которой указывается в @base
	,@base int = 16
	,@dec_inc int = 5 -- десятичный инкремент

--
set nocount on

declare
	@array varchar(100) = '0,1,2,3,4,5,6,7,8,9,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z'
	,@separator varchar(1) = ','
	,@dec bigint
	,@out varchar(100)

declare @digits table (num int)

;with ten as (
	select n
	from (values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) as t(n)
),cte as (
	select row_number() over(order by (select null)) as n
	from
		ten as t1
		cross join ten as t2
		cross join ten as t3
)
insert into @digits (num)
select n from cte

declare @dict table (
	ord int
	,val varchar(1)
)

insert into @dict (ord, val)
select top (@base)
	row_number() over(order by (select null)) - 1
	,cast(s.[value] as varchar(1))
from string_split(@array, @separator) as s

;with t as (
	select top (len(@number))
		t.num
		,substring(@number, row_number() over(order by t.num), 1) as val
	from @digits as t
	order by t.num
), s as ( 
	select d.ord * power(@base, row_number() over(order by t.num desc) - 1) as val
	from
		t
		inner join @dict as d
			on t.val = d.val
)
select @dec = sum(val)
from s

set @dec += @dec_inc

;with rec as (
	select
		@dec / @base as [dec]
		,(select val from @dict where ord = @dec % @base) as val
		,0 as ord
	union all
	select
		r.[dec] / @base
		,d.val
		,r.ord + 1
	from
		rec as r
		inner join @dict as d
			on d.ord = r.[dec] % @base
	where not (r.[dec] / @base = 0 and r.[dec] % @base = 0)
)
select @out = string_agg(val, '') within group (order by ord desc)
from rec

select @out as result


