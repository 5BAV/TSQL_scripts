--создается триггер на таблице
create or alter trigger tr_a1 on dbo.A1
instead of delete
as
if @@rowcount = 0
	return
set nocount on

declare @sc sql_variant = session_context(N'deletion')

if (@sc is null)
	update a set
		a.quantity = 0
	from Deleted as d
	inner join dbo.A1 as a
		on a.id = d.id
	where a.quantity <> 0
else if (@sc = 'on')
	delete a
	from Deleted as d
	inner join dbo.A1 as a
		on a.id = d.id
	where a.quantity = 0
else if (@sc = 'all')
	delete a
	from Deleted as d
	inner join dbo.A1 as a
		on a.id = d.id
go

--для управления задать контекст
begin tran
exec sys.sp_set_session_context N'deletion', 'all'
select * from dbo.A1
delete dbo.A1 where id < 3
select * from dbo.A1
rollback


select
*
from dbo.all_type_test as att