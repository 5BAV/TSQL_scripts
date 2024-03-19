--СОЗДАНИЕ ТАБЛИЦЫ ДЛЯ ХРАНЕНИЯ ЛОГОВ
drop table if exists dbo.log_table
go

create table dbo.log_table (
	[Id] int identity
	,[Trigger] sysname
	,[Session] int
	,[Login] nvarchar(300)
	,[Application] nvarchar(128)
	,[Event_type] nvarchar(256)
	,[SQL] nvarchar(max)
	,[DT_stamp] datetime2 default sysutcdatetime()
	primary key
)
go

--СОЗДАНИЕ ТРИГГЕРА НА ОТСЛЕЖИВАЕМОЙ ТАБЛИЦЕ
create or alter trigger tr_for_log_table on dbo.[table]
after insert, update, delete
as
if @@rowcount = 0
	return

set nocount on
set xact_abort off

begin try

--запись в лог
	insert into dbo.log_table ([Trigger], [Session], [Login], [Application], [Event_type], [SQL])
	select
		object_name(@@procid)
		,@@spid
		,upper(concat('[', current_user, '] ',substring(original_login(), charindex('\', original_login()) + 1, 128)))
		,app_name()
		,event_type
		,event_info
	from sys.dm_exec_input_buffer (@@spid,null)

--запись в события
	raiserror('callstack_for_table', 11, 1)

end try
begin catch
end catch

go

--СОЗДАНИЕ СЕССИИ СОБЫТИЙ
if exists (select 1 from sys.server_event_sessions where name = 'callstack_for_table')
	drop event session callstack_for_table on server
go

create event session callstack_for_table on server 
add event sqlserver.error_reported (
	action (
		sqlserver.client_app_name
		,sqlserver.plan_handle
		,sqlserver.session_nt_username
		,sqlserver.sql_text
		,sqlserver.tsql_frame
		,sqlserver.tsql_stack
		,sqlserver.session_id
		,package0.process_id
	)
	where ([message] = N'callstack_for_table')
)
add target package0.event_file (
	set filename = N'callstack_for_table.xel'
	,max_file_size = (50)
	,max_rollover_files = (1)
),
add target package0.ring_buffer (
	set max_events_limit=(5000)
	,max_memory=(51200)
)
with (
	max_memory = 4096 kb
	,event_retention_mode = allow_multiple_event_loss
	,max_dispatch_latency = 3 seconds
	,max_event_size = 0 kb
	,memory_partition_mode = none
	,track_causality = off
	,startup_state = off
)
go

alter event session callstack_for_table on server
	state = start
go

--ПРОСМОТР СОБЫТИЙ
set tran isolation level read uncommitted

select
    se.[name] as [session-name]
    ,ev.event_name
    ,ac.action_name
    ,st.target_name
    ,se.session_source
    ,st.target_data
    ,cast(st.target_data as xml) as [target_data_XML]
from
	sys.dm_xe_session_event_actions as ac
    inner join sys.dm_xe_session_events as ev
		on ev.event_name = ac.event_name
        and ev.event_session_address = ac.event_session_address
    inner join sys.dm_xe_session_object_columns as oc
         on oc.event_session_address = ac.event_session_address
    inner join sys.dm_xe_session_targets as st
         on st.event_session_address = ac.event_session_address
    inner join sys.dm_xe_sessions as se
         on ac.event_session_address = se.[address]
where
    se.[name] = 'callstack_for_table'
    and ac.action_name = 'sql_text'
	and oc.column_name = 'occurrence_number'
order by 
	se.[name],
    ev.event_name,
    ac.action_name,
    st.target_name,
    se.session_source

--из файла
;with fxml as (
	select
		cast(event_data as xml) as event_data
		,timestamp_utc
	from sys.fn_xe_file_target_read_file('callstack_for_table*0.xel',null, null, null)
), result as (
	select
		fxml.timestamp_utc
		,fxml.event_data.value('(/event/data[@name = "message"]/value)[1]', 'varchar(20)') as [message]
		,fxml.event_data.value('(/event/action[@name = "session_id"]/value)[1]', 'int') as [session_id]
		,fxml.event_data.value('(/event/action[@name = "process_id"]/value)[1]', 'int') as [process_id]
		,fxml.event_data.value('(/event/action[@name = "sql_text"]/value)[1]', 'varchar(max)') as [sql_text]
		,fxml.event_data.value('(/event/action[@name = "session_nt_username"]/value)[1]', 'varchar(300)') as [session_nt_username]
		,fxml.event_data.value('(/event/action[@name = "client_app_name"]/value)[1]', 'varchar(300)') as [client_app_name]
		,frame.[name].[value]('@level','int') as [level]
		,convert(varbinary(max), frame.[name].[value]('@handle','varchar(max)'), 1) AS [handle]
from	
	fxml
	outer apply	fxml.event_data.nodes('/event/action[@name = "tsql_stack"]/value/frames/frame') as frame([name])
)
select
	result.timestamp_utc
    ,result.[session_id]
    ,result.process_id
    ,result.sql_text
    ,result.session_nt_username
    ,result.client_app_name
    ,result.[level]
	,object_name(txt.objectid) as [objectid]
from
	result
	cross apply sys.dm_exec_sql_text(result.handle) as txt
order by
	result.[session_id]
	,result.timestamp_utc
	,result.[level]

--из буфера
declare	@xml xml = (
	select convert(xml, target_data)
	from
		sys.dm_xe_sessions as s 
		inner join sys.dm_xe_session_targets as t 
			on t.event_session_address = s.[address]
	where
		s.[name] = 'callstack_for_table'
		and t.target_name = N'ring_buffer'
	)

select
	rb.ev.[value]('(@timestamp)', 'datetime') as [timestamp]
	,rb.ev.[value]('(//data[@name = "message"]/value)[1]', 'varchar(20)') as [message]
	,rb.ev.[value]('(//action[@name = "session_id"]/value)[1]', 'int') as [session_id]
	,rb.ev.[value]('(//action[@name = "process_id"]/value)[1]', 'int') as [process_id]
	,rb.ev.[value]('(//action[@name = "sql_text"]/value)[1]', 'varchar(max)') as [sql_text]
	,rb.ev.[value]('(//action[@name = "session_nt_username"]/value)[1]', 'varchar(300)') as [session_nt_username]
	,rb.ev.[value]('(//action[@name = "client_app_name"]/value)[1]', 'varchar(300)') as [client_app_name]
	,h.[level]
	,h.[handle]
	,h.[sql_text]
	,h.obj_name
from
	@xml.nodes('//RingBufferTarget/event') as rb(ev)
	outer apply (
		select
			frame.[name].[value]('(@level)','int') as [level]
			,convert(varbinary(max), frame.[name].[value]('(@handle)','varchar(max)'), 1) as [handle]
			,tx.[text] as [sql_text]
			,object_name(tx.objectid) as [obj_name]
		from
			rb.ev.nodes('//action[@name = "tsql_stack"]/value/frames/frame') as frame([name])
			outer apply sys.dm_exec_sql_text(convert(varbinary(max), frame.[name].[value]('(@handle)','varchar(max)'), 1)) as tx
		where rb.ev.value('(@timestamp)', 'datetime') = frame.[name].[value]('(../../../../@timestamp)', 'datetime')
	) as h
order by
	[timestamp]
	,h.[level]
