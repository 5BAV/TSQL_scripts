set tran isolation level read uncommitted
set nocount, xact_abort on 

drop table if exists #pi
create table #pi (
	[db name] sysname
	,[obj name] sysname
	,[idx name] sysname
	,[partition id] nvarchar(30)
)

declare @query varchar(max)

select @query = string_agg('
	select
		''' + [name] + '''
		,o.[name]
		,isnull(i.[name], '''')
		,p.[partition_id]
	from
		' + quotename([name]) + '.sys.partitions as p
		inner join ' + quotename([name]) + '.sys.objects as o
			on o.[object_id] = p.[object_id]
		left join ' + quotename([name]) + '.sys.indexes as i
			on i.[index_id] = p.[index_id]
			and i.[object_id] = p.[object_id]
	', char(10))
from sys.databases

insert #pi
exec (@query)

;with cte as (
	select distinct
		cast(0 as smallint) as [who]
		,er.blocking_session_id as [whom]
		,0 as ord
		,ss.database_id
	from
		sys.dm_exec_requests as er
		left join sys.dm_exec_sessions as ss
			on er.blocking_session_id = ss.session_id
	where
		blocking_session_id <> 0
		and not exists (
			select 1
			from sys.dm_exec_requests as er2 where er2.blocking_session_id <> 0 and er.blocking_session_id = er2.[session_id]
		)
	union all
	select
		er.blocking_session_id
		,er.[session_id]
		,bs.ord + 1
		,bs.database_id
	from
		sys.dm_exec_requests as er
		inner join cte as bs
			on bs.[whom] = er.blocking_session_id
)
select
	[бд сессии] = db_name(isnull(rq.database_id, ss.database_id))
	,[сессия] = cte.whom
	,[кто блок.] = cte.who
	,[очередн. блок.] = cte.ord
	,[команда] = isnull(rq.command, '')
	,[ур. изоляции] = case rq.transaction_isolation_level
		when 1 then 'uncommitted'
		when 2 then 'committed'
		when 3 then 'repeatable'
		when 4 then 'serializable'
		when 5 then 'snapshot'
			else '-' end
	,[начало запроса] = isnull(format(rq.start_time, 'G'), '')
	,[начало транз.] = isnull(format(actr.transaction_begin_time, 'G'), '')
	,[продолж. блок.] = iif(rq.total_elapsed_time is null, '', concat((rq.total_elapsed_time / 86400000) % 24 ,'.',(rq.total_elapsed_time / 3600000) % 24 ,':',(rq.total_elapsed_time / 60000) % 60 ,':',(rq.total_elapsed_time / 1000) % 60 ))
	,[статус запроса] = isnull(rq.[status], '')
	,[состояние тран.] = case actr.transaction_state
		when 0 then 'not inint'
		when 1 then 'init'
		when 2 then 'active'
		when 3 then 'read done'
		when 4 then 'distrib. tran'
		when 5 then 'done'
		when 6 then 'commited'
		when 7 then 'rollback'
		when 8 then 'canceled'
		else '' end
	,[ожидание] = isnull(rq.wait_type + ' (' + rq.last_wait_type + ')', '')
	,[заблок. ресурс] = isnull(rq.wait_resource, '')
	,[описание] = isnull(quotename(objres.[db name]) + '.' + quotename(objres.[obj name]) + '.' + quotename(objres.[idx name]), '')
	,[пользователь] = ss.login_name
	,[хост] = ss.[host_name]
	,[приложение] = ss.[program_name]
	,[адрес] = cn.client_net_address
	,[вложенность] = isnull(rq.nest_level, 0)
	,[план] = isnull(pln.query_plan, '')
	,[тип тран.] = case actr.transaction_type
		when 1 then 'RW'
		when 2 then 'RO'
		when 3 then 'system'
		when 4 then 'distrib. tran'
		else '' end
	,[схема лочки] = isnull(tr.tran_locks, '')
	,[контекст] = isnull(rq.[context_info], 0x0)
	,[логич. чтений] = isnull(rq.logical_reads, 0)
	,[Время проц] = iif(rq.cpu_time is null, '', concat((rq.cpu_time / 86400000) % 24 ,'.',(rq.cpu_time / 3600000) % 24 ,':',(rq.cpu_time / 60000) % 60 ,':',(rq.cpu_time / 1000) % 60 ))
	,[физ. записей] = isnull(rq.writes, 0)
	,[физ. чтений] = isnull(rq.reads, 0)
	,[ожид. блок.] = iif(rq.[lock_timeout] > 0, concat(rq.[lock_timeout], ' мс'), 'бесконечно') 
	,[приоритет блок.] = isnull(rq.[deadlock_priority], 0)
	,[степ. паралл.] = isnull(rq.dop, 0)
	,[счетч. транз.] = ss.open_transaction_count
	,[запрос] = isnull(substring(txt.[text], (rq.statement_start_offset / 2) + 1,(( case rq.statement_end_offset when -1 then datalength(txt.text) else rq.statement_end_offset end - rq.statement_start_offset) / 2) + 1), '')
	,[запрос полный] = isnull(txt.[text], '')
	,[строк сесс.] = isnull(ss.row_count, 0)
	,[статус сесс.] = isnull(ss.[status], '')
	,[память сесс.] = concat(ss.memory_usage * 8, ' KB')
from
	cte
	left join sys.dm_exec_sessions as ss
		on cte.whom = ss.session_id
	left join sys.dm_exec_requests as rq
		on cte.whom = rq.session_id
	left join sys.dm_exec_connections as cn
		on cte.whom = cn.session_id
	--left join sys.dm_tran_active_snapshot_database_transactions as tas
	--	on tas.session_id = ss.session_id
	left join sys.dm_tran_active_transactions as actr
		on rq.transaction_id = actr.transaction_id
	outer apply sys.dm_exec_sql_text(isnull(rq.sql_handle,cn.most_recent_sql_handle)) as txt
	outer apply sys.dm_exec_query_plan(rq.plan_handle) as pln
	outer apply (
		select
			[tran].request_owner_id as [id],
			[tran].request_mode as [mode],
			[tran].request_type as [type],
			[tran].request_status as [status],
			[tran].resource_type as [resource],
			iif([tran].resource_type = 'object',object_name([tran].resource_associated_entity_id),'') as [name],
			iif([tran].resource_type = 'object',objectpropertyex([tran].resource_associated_entity_id,'basetype'),'') as [base_type],
			rtrim([tran].resource_description) as [description]
		from
			sys.dm_tran_locks as [tran]
			inner join sys.dm_tran_locks as t2
				on t2.resource_type = [tran].resource_type
				and t2.resource_subtype = [tran].resource_subtype
				and t2.resource_database_id = [tran].resource_database_id
				and t2.resource_description = [tran].resource_description
				and t2.resource_associated_entity_id = [tran].resource_associated_entity_id
				and t2.resource_lock_partition = [tran].resource_lock_partition
		where
			[tran].request_owner_type = 'transaction'
			and [tran].request_session_id = cte.whom 
			and t2.request_session_id = cte.who
		for xml path('tran'), type, root('tran_locks')
	) as tr(tran_locks)
	outer apply (
		select *
		from #pi as p
		where rq.wait_resource like 'key%'
			and db_name(cte.database_id) = p.[db name]
			and rq.wait_resource like '%' + p.[partition id] + '%'
	) as objres
where
	cn.parent_connection_id is null
order by
	cte.ord
	,cte.who
	,cte.whom
option (recompile)

