set nocount, xact_abort on
set tran isolation level read committed

if object_id('dbo.log_table', 'U') is null
	exec ('
		create table dbo.log_table (
			ID int,
			[OLD E-Mail] nvarchar(300),
			[NEW E-Mail] nvarchar(300),
			primary key (ID)
		)
	')
go

update t set
	t.[E-Mail] = s.[NEW E-Mail]
output
	Inserted.ID
	,Deleted.[E-Mail]
	,Inserted.[E-Mail]
into dbo.log_table (ID, [OLD E-Mail], [NEW E-Mail])
from
	dbo.[TABLE] as t
	cross apply (
		select string_agg([value], '; ')
		from string_split(
				replace(replace(replace(replace(replace(replace(replace(replace( t.[E-Mail]
				,char(32),'|'),char(9),'|'),'\','|'),'/','|'),';','|'),',','|'),'<','|'),'>','|')
			, '|')
		where [value] like '%@%'
	) as s([NEW E-Mail])
where 1=1
	and t.[E-Mail] like '%@%'
	and t.[E-Mail] <> s.[NEW E-Mail]

print concat('ОБРАБОТАНО СТРОК:', @@rowcount)

