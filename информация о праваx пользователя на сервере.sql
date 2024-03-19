--ПОДГОТОВКА
/*
use [master]
go

exec master.dbo.sp_addlinkedserver
	@server = N'ActDirSrvc',
	@srvproduct = N'Active Directory Services',
	@provider = N'ADsDSOObject',
	@datasrc = N'' --domain.local
go

exec master.dbo.sp_addlinkedsrvlogin
	@rmtsrvname = N'ActDirSrvc'
	,@useself = N'False'
	,@locallogin = null
	,@rmtuser = N'' --AD login
	,@rmtpassword = N'' --password
go
*/

--ПОЛУЧЕНИЕ ДАННЫХ
set tran isolation level read uncommitted
set nocount on

declare
	@user_name nvarchar(max) = '' --искомый пользователь (domain\username)
	,@domain nvarchar(max) = '' --домен (domain.local)

declare
	@query nvarchar(max)
	,@wgr nvarchar(max)
	,@dc nvarchar(max)
	,@name nvarchar(max)

select @dc = string_agg('DC=' + [value], ',')
from string_split(@domain, '.')

drop table if exists #AD
create table #AD (
	[Group name] nvarchar(300)
	,[User name] nvarchar(300)
)

declare cur cursor for
	select distinct substring([name], charindex('\', [name]) + 1, len([name]))
	from master.sys.server_principals
	where [type_desc] = 'WINDOWS_GROUP'

open cur
fetch next from cur into @wgr

while @@fetch_status = 0
begin

	set @query = '
		select @name = distinguishedName
		from openrowset(''ADSDSOObject'','''',''
			select distinguishedName
			from ''''LDAP://' + @dc + '''''
			where
				objectCategory = ''''Group''''
				and SAMAccountName = ''''' + @wgr + '''''
		'')'

	exec sp_executesql
		@query
		,N'@name nvarchar(max) output'
		,@name = @name output

	set @query = '
		select @name = SAMAccountName
		from openrowset(''ADSDSOObject'','''',''
			select SAMAccountName
			from ''''LDAP://' + @dc + '''''
			where
				objectCategory = ''''Person''''
				and objectClass = ''''user'''' 
				and memberOf = ''''' + @name + '''''
		'')'
	exec sp_executesql
		@query
		,N'@name nvarchar(max) output'
		,@name = @name output

	insert into #AD ([Group name], [User name])
	select
		reverse(parsename(reverse(@domain), 1)) + '\' + @wgr
		,reverse(parsename(reverse(@domain), 1)) + '\' + @name

    fetch next from cur into @wgr
end

close cur
deallocate cur

drop table if exists #DBR
create table #DBR (
	[DB name] sysname
	,[Role name] sysname
	,[Role member] sysname
	,[Is fixed role] int
)

drop table if exists #DBP
create table #DBP (
	[DB name] sysname
	,[Class desc] nvarchar(128)
	,[Permission name] sysname
	,[State desc] nvarchar(128)
	,[Principal name] sysname
	,[Principal type] nvarchar(128)
	,[Object name] sysname
	,[Column name] nvarchar(128)
)

declare cur cursor for
	select [name]
	from sys.databases
	where database_id not in (2, 3, 4)

open cur
fetch next from cur into @name

while @@fetch_status = 0
begin

	set @query = '
		use [' + @name + ']

		select
			db_name()
			,dpr.[name]
			,dpm.[name]
			,dpr.[is_fixed_role]
		from
			sys.database_role_members as drm
			left join sys.database_principals as dpr
				on drm.role_principal_id = dpr.principal_id
			left join sys.database_principals as dpm
				on drm.member_principal_id = dpm.principal_id
		where dpr.[type] = ''R''
	'

	insert into #DBR ([DB name], [Role name], [Role member], [Is fixed role])
	exec(@query)

	set @query = '
		use [' + @name + ']

		select
			db_name()
			,dbp.[class_desc]
			,dbp.[permission_name]
			,dbp.[state_desc]
			,dbr.[name]
			,dbr.[type_desc]
			,quotename(schema_name(o.[schema_id])) + ''.'' + quotename(o.[name])
			,c.[name]
		from
			sys.database_permissions as dbp
			inner join sys.database_principals as dbr
				on dbp.grantee_principal_id = dbr.principal_id
			inner join sys.all_objects as o
				on dbp.major_id = o.[object_id]
			left join sys.columns as c
				on c.[object_id] = dbp.major_id
				and c.[column_id] = dbp.minor_id
		'

	insert into #DBP ([DB name], [Class desc], [Permission name], [State desc], [Principal name], [Principal type], [Object name], [Column name])
	exec(@query)

	fetch next from cur into @name
end
close cur
deallocate cur

--серверные права
select
	spm.class_desc as ClassDesc
	,spm.state_desc as StateDesc
	,spm.[permission_name] as PermissionName
	,spr.[name] as PrincipalName
	,isnull(ad.[User name], '') as [Group member]
	,spm.major_id as ObjectId
from
	sys.server_permissions as spm
	inner join sys.server_principals as spr
		on spm.grantee_principal_id = spr.principal_id
	left join #AD as ad
		on spr.[name] = ad.[Group name]
where 
	spr.[name] = @user_name
	or ad.[User name] = @user_name
	or spr.[name] = 'public'

--серверные роли
select
	isnull(ad.[User name],'') as [Group member]
	,spr.[name] as [Role name]
	,spm.[name] as [Role member]
	,spr.is_fixed_role as [Is fixed role]
from
	sys.server_role_members as srm
	left join sys.server_principals as spm
		on srm.member_principal_id = spm.principal_id
	left join sys.server_principals as spr
		on srm.role_principal_id = spr.principal_id
	left join #AD as ad
		on spm.[name] = ad.[Group name]
where
	spr.[type] = 'R'
	and (spm.[name] = @user_name or ad.[User name] = @user_name)

--роли БД
select
	dbr.[DB name]
	,dbr.[Role name]
	,dbr.[Role member]
	,ad.[User name]
	,dbr.[Is fixed role]
from	
	#DBR as dbr
	left join #AD as ad
		on dbr.[Role member] = ad.[Group name]
where
	dbr.[Role member] = @user_name
	or ad.[User name] = @user_name

--права БД
select
	dbp.[DB name]
	,dbp.[Class desc]
	,dbp.[State desc]
	,dbp.[Permission name]
	,dbp.[Principal type]
	,dbp.[Principal name]
	,dbr.[Role member] as [Group member]
	,ad.[User name] as [Group member]
	,dbp.[Object name]
	,dbp.[Column name]
from
	#DBP as dbp
	left join #DBR as dbr
		on dbp.[DB name] = dbr.[DB name]
		and dbp.[Principal name] = dbr.[Role name]
	left join #AD as ad
		on dbp.[Principal name] = ad.[Group name]
where
	dbp.[Principal name] = @user_name
	or dbr.[Role member] = @user_name
	or ad.[User name] = @user_name
	or dbp.[Principal name] = 'public'
order by
	dbp.[DB name]
	,dbp.[Principal name]
	,dbp.[State desc]
	,dbp.[Class desc]
