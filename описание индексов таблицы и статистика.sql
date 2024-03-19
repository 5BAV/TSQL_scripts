set nocount, xact_abort on
set tran isolation level read uncommitted

declare @schema_table sysname = 'dbo.table'

drop table if exists #Buffs;
create table #Buffs (
    db_id int not null
    ,allocation_unit_id bigint not null
    ,size decimal(12, 3) not null
    ,primary key ( db_id, allocation_unit_id )
);

drop table if exists #OpStats;
create table #OpStats (
	database_id smallint not null
	,[object_id] int not null
	,index_id int not null
	,range_scan_count bigint null
	,singleton_lookup_count bigint null
	,forwarded_fetch_count bigint null
	,lob_fetch_in_pages bigint null
	,row_overflow_fetch_in_pages bigint null
	,leaf_insert_count bigint null
	,leaf_update_count bigint null
	,leaf_delete_count bigint null
	,leaf_ghost_count bigint null
	,nonleaf_insert_count bigint null
	,nonleaf_update_count bigint null
	,nonleaf_delete_count bigint null
	,leaf_allocation_count bigint null
	,nonleaf_allocation_count bigint null
	,row_lock_count bigint null
	,row_lock_wait_count bigint null
	,row_lock_wait_in_ms bigint null
	,page_lock_count bigint null
	,page_lock_wait_count bigint null
	,page_lock_wait_in_ms bigint null
	,index_lock_promotion_attempt_count bigint null
	,index_lock_promotion_count bigint null
	,page_latch_wait_count bigint null
	,page_latch_wait_in_ms bigint null
	,tree_page_latch_wait_count bigint null
	,tree_page_latch_wait_in_ms bigint null
	,page_io_latch_wait_count bigint null
	,page_io_latch_wait_in_ms bigint null
	,page_compression_attempt_count bigint null
	,page_compression_success_count bigint null
	,primary key ( database_id, [object_id], index_id )
)

insert into #Buffs (db_id, allocation_unit_id, size)
select
	database_id
	,allocation_unit_id
	,convert(decimal(12, 3), count(*) / 128.0)
from sys.dm_os_buffer_descriptors with ( nolock )
group by
	database_id
	,allocation_unit_id
option (maxdop 1)

insert into #OpStats (
	database_id
	,[object_id]
	,index_id
	,range_scan_count
	,singleton_lookup_count
	,forwarded_fetch_count
	,lob_fetch_in_pages
	,row_overflow_fetch_in_pages
	,leaf_insert_count
	,leaf_update_count
	,leaf_delete_count
	,leaf_ghost_count
	,nonleaf_insert_count
	,nonleaf_update_count
	,nonleaf_delete_count
	,leaf_allocation_count
	,nonleaf_allocation_count
	,row_lock_count
	,row_lock_wait_count
	,row_lock_wait_in_ms
	,page_lock_count
	,page_lock_wait_count
	,page_lock_wait_in_ms
	,index_lock_promotion_attempt_count
	,index_lock_promotion_count
	,page_latch_wait_count
	,page_latch_wait_in_ms
	,tree_page_latch_wait_count
	,tree_page_latch_wait_in_ms
	,page_io_latch_wait_count
	,page_io_latch_wait_in_ms
	,page_compression_attempt_count
	,page_compression_success_count
)
select
	os.database_id
	,os.[object_id]
	,os.index_id
	,sum(os.range_scan_count) as range_scan_count
	,sum(os.singleton_lookup_count) as singleton_lookup_count
	,sum(os.forwarded_fetch_count) as forwarded_fetch_count
	,sum(os.lob_fetch_in_pages) as lob_fetch_in_pages
	,sum(os.row_overflow_fetch_in_pages) as row_overflow_fetch_in_pages
	,sum(os.leaf_insert_count) as leaf_insert_count
	,sum(os.leaf_update_count) as leaf_update_count
	,sum(os.leaf_delete_count) as leaf_delete_count
	,sum(os.leaf_ghost_count) as leaf_ghost_count
	,sum(os.nonleaf_insert_count) as nonleaf_insert_count
	,sum(os.nonleaf_update_count) as nonleaf_update_count
	,sum(os.nonleaf_delete_count) as nonleaf_delete_count
	,sum(os.leaf_allocation_count) as leaf_allocation_count
	,sum(os.nonleaf_allocation_count) as nonleaf_allocation_count
	,sum(os.row_lock_count) as row_lock_count
	,sum(os.row_lock_wait_count) as row_lock_wait_count
	,sum(os.row_lock_wait_in_ms) as row_lock_wait_in_ms
	,sum(os.page_lock_count) as page_lock_count
	,sum(os.page_lock_wait_count) as page_lock_wait_count
	,sum(os.page_lock_wait_in_ms) as page_lock_wait_in_ms
	,sum(os.index_lock_promotion_attempt_count) as index_lock_promotion_attempt_count
	,sum(os.index_lock_promotion_count) as index_lock_promotion_count
	,sum(os.page_latch_wait_count) as page_latch_wait_count
	,sum(os.page_latch_wait_in_ms) as page_latch_wait_in_ms
	,sum(os.tree_page_latch_wait_count) as tree_page_latch_wait_count
	,sum(os.tree_page_latch_wait_in_ms) as tree_page_latch_wait_in_ms
	,sum(os.page_io_latch_wait_count) as page_io_latch_wait_count
	,sum(os.page_io_latch_wait_in_ms) as page_io_latch_wait_in_ms
	,sum(os.page_compression_attempt_count) as page_compression_attempt_count
	,sum(os.page_compression_success_count) as page_compression_success_count
from sys.dm_db_index_operational_stats(null, null, null, 0) as os
where os.database_id = db_id()
group by
	os.database_id
    ,os.object_id
    ,os.index_id
option (maxdop 1)

;with TableInfo as (
	select
		t.[object_id]
		,i.index_id
		,sch.[name] + '.' + t.[name] as [table]
		,quotename(i.[name]) as [index]
		,i.[type_desc] as [type]
		,sum(p.[rows]) as [rows]
		,i.is_unique 
		,i.fill_factor
		,i.is_disabled
		,i.filter_definition as [filter]
		,t.lock_escalation_desc as [lock_escalation]
		,case max(p.data_compression)
			when 0 then 'NONE'
			when 1 then 'ROW'
			when 2 then 'PAGE'
			when 3 then 'COLUMNSTORE'
			when 4 then 'COLUMNSTORE_ARCHIVE'
				end as [max_compression]
		,sum(a.total_pages) as [total_pages]
		,sum(a.used_pages) as [used_pages]
		,sum(a.data_pages) as [data_pages]
		,convert(decimal(12,3),sum(a.total_pages) * 8. / 1024.) as [total_space_mb]
		,convert(decimal(12,3),sum(a.used_pages) * 8. / 1024.) as [used_space_mb]
		,convert(decimal(12,3),sum(a.data_pages) * 8. / 1024.) as [data_space_mb]
		,sum(bi.size) as [buffer_pool_space_mb]
	from
		sys.tables as t
		inner join sys.indexes as i
			on t.[object_id] = i.[object_id]
		inner join sys.partitions as p
			on i.[object_id] = p.[object_id]
			and i.index_id = p.index_id
		inner join sys.allocation_units as a
			on p.[partition_id] = a.container_id
		inner join sys.schemas as sch
			on t.[schema_id] = sch.[schema_id]
		left join #Buffs as bi
			on bi.[db_id] = db_id()
			and a.allocation_unit_id = bi.allocation_unit_id 
	where i.[object_id] > 255 
	group by
		sch.[name]
		,t.[name]
		,i.[type_desc]
		,i.[object_id]
		,i.index_id
		,i.[name]
		,i.is_unique
		,i.fill_factor
		,t.lock_escalation_desc
		,t.[object_id]
		,i.index_id
		,i.filter_definition
		,i.is_disabled
)
select 
	ti.[object_id]
	,ti.index_id
	,ti.[table]
	,ti.[index]
	,iif(ic.is_guid = 1, 'yes', 'no') as is_guid
	,ti.[type]
	,left(idx_def.key_col, len(idx_def.key_col) - 1) as [key_columns]
	,left(idx_def.included_col, len(idx_def.included_col) - 1) as [included_columns]
	,ti.[filter]
	,idx_len.max_key_length
	,ti.[rows]
	,ti.is_unique
	,ti.is_disabled
	,ti.[lock_escalation]
	,ti.max_compression
	,ti.total_pages
	,ti.used_pages
	,ti.data_pages
	,ti.total_space_mb
	,ti.used_space_mb
	,ti.data_space_mb
	,ti.buffer_pool_space_mb
	,stats_date(ti.[object_id], ti.index_id) as [stats_date]
	,ius.user_seeks
	,ius.user_scans
	,ius.user_lookups
	,ius.user_seeks + ius.user_scans + ius.user_lookups AS [user_reads]
	,ius.user_updates 
	,ius.last_user_seek
	,ius.last_user_scan
	,ius.last_user_lookup
	,ius.last_user_update
	,ius.profit
	,ios.range_scan_count
	,ios.singleton_lookup_count
	,ios.forwarded_fetch_count
	,ios.lob_fetch_in_pages
	,ios.row_overflow_fetch_in_pages
	,ios.leaf_insert_count
	,ios.leaf_update_count
	,ios.leaf_delete_count
	,ios.leaf_ghost_count
	,ios.nonleaf_insert_count
	,ios.nonleaf_update_count
	,ios.nonleaf_delete_count
	,ios.leaf_allocation_count
	,ios.nonleaf_allocation_count
	,ios.row_lock_count
	,ios.row_lock_wait_count
	,ios.row_lock_wait_in_ms
	,ios.page_lock_count
	,ios.page_lock_wait_count
	,ios.page_lock_wait_in_ms
	,ios.index_lock_promotion_attempt_count
	,ios.index_lock_promotion_count
	,ios.page_latch_wait_count
	,ios.page_latch_wait_in_ms
	,ios.tree_page_latch_wait_count
	,ios.tree_page_latch_wait_in_ms
	,ios.page_io_latch_wait_count
	,ios.page_io_latch_wait_in_ms
	,ios.page_compression_attempt_count
	,ios.page_compression_success_count
from
	TableInfo as ti
	left join #OpStats as ios
		on ti.[object_id] = ios.[object_id]
		and ti.index_id = ios.index_id
		and ios.database_id = db_id()
	outer apply (
		select 
			ius.user_seeks
			,ius.user_scans
			,ius.user_lookups
			,ius.user_updates 
			,ius.last_user_seek
			,ius.last_user_scan
			,ius.last_user_lookup
			,ius.last_user_update
			,10000 * ius.user_seeks / (ius.user_updates + 1) as profit
		from sys.dm_db_index_usage_stats as ius
		where
			ius.database_id = db_id()
			and ius.[object_id] = ti.[object_id]
			and ius.index_id = ti.index_id
		) as ius
	--outer apply (
	--			select sum(ps.[used_page_count]) * 8 / 1024 as IDX_Size_MB
	--			from sys.dm_db_partition_stats as ps
	--			where
	--				ps.[object_id] = ti.[object_id]
	--				and ps.index_id = ti.index_id
	--) as isz
	outer apply (
		select top (1) 1 as is_guid
		from
			sys.index_columns as ic
			inner join sys.columns as c
				on ic.[object_id] = c.[object_id]
				and	ic.column_id = c.column_id
		where
			ic.[object_id] = ti.[object_id]
			and ic.index_id = ti.index_id
			and c.system_type_id = 36
	) as ic
	outer apply (
		select
			(	select 
					quotename(col.[name]) as [text()]
					,iif(icol_meta.is_descending_key = 1, ' DESC','') as [text()]
					,',' as [text()]
				from
					sys.index_columns as icol_meta
					inner join sys.columns as col
						on icol_meta.[object_id] = col.[object_id]
						and icol_meta.column_id = col.column_id
				where
					icol_meta.[object_id] = ti.[object_id]
					and icol_meta.index_id = ti.index_id
					and icol_meta.is_included_column = 0
				order by icol_meta.key_ordinal
				for xml path('')
			) as key_col
			,(	select
					quotename(col.[name]) as [text()]
					,',' as [text()]
				from
					sys.index_columns as icol_meta
					inner join sys.columns as col
						on icol_meta.[object_id] = col.[object_id]
						and icol_meta.column_id = col.column_id
				where
					icol_meta.[object_id] = ti.[object_id]
					and icol_meta.index_id = ti.index_id
					and icol_meta.is_included_column = 1
				order by col.[name]
				for xml path('')
			) as included_col
	) as idx_def
	outer apply (
		select sum(c.max_length) as max_key_length
		from
			sys.index_columns as ic
			inner join sys.columns as c
				on ic.[object_id] = c.[object_id]
				and ic.column_id = c.column_id
		where
			ic.[object_id] = ti.[object_id]
			and ic.index_id = ti.index_id
			and ic.is_included_column = 0
	) as idx_len
where ti.[table] = replace(replace(@schema_table, '[', ''), ']', '')
option (recompile, maxdop 1)

