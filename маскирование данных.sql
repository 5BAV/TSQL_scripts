
--ВСТРОЕННОЕ РЕШЕНИЕ ДЛЯ SQLSERVER 2016+ МАСКИРОВАНИЕ ВЫВОДА

drop table if exists dbo.Consultants

create table dbo.Consultants (
	timestamp,
	ID int not null,
	FirstName varchar(32) masked with (function='partial(1,"XXXXXXXX",0)') not null,
	Pasport varchar(32) masked with  (function='partial(1,"XXXXXXX",1)') not null,
	DateOfBirth date masked with (function='default()') not null,
	SSN char(12) masked with (function='partial(0,"XXX-XXX-",4)') not null,
	EMail nvarchar(255) masked with (function='email()') not null,
	SpendingLimit money	masked with (function='random(500,1000)') not null,
	primary key (Pasport)
)

insert into dbo.Consultants (ID, FirstName, Pasport, DateOfBirth, SSN, Email, SpendingLimit)
values
	(1,'Andrei','5544001122','19800505','123-456-7890','am@mail.com',10000),
	(2,'Kirill','2277332211','20000707','234-567-8901','kne@mailservice.net',5000)

create user NonPrivUser without login

grant select on dbo.Consultants to NonPrivUser
go

select * from dbo.Consultants

execute as user = 'NonPrivUser'
select * from dbo.Consultants
revert





--ЧАСТНОЕ РЕШЕНИЕ МАСКИРОВАНИЕ В ТАБЛИЦАХ

--создание шаблонов масок
drop table if exists dbo.[Data Masking Template]

select *
into dbo.[Data Masking Template]
from (values
	(1,'address','default',0),
	(2,'birthday','19000101',0),
	(3,'description/comment','',0),
	(4,'e-mail','box@email.local',0),
	(5,'name','XXXXX',0),
	(6,'passport','111111111',3),
	(7,'phone','1111111',3),
	(8,'web-site','default.local',0),
	(9,'ИНН','111111111111',1),
	(10,'ОГРН','111111111111111',1),
	(11,'Pin-Code','1-9',2)
) as t([Entry No_], [Template Name], [Mask], [Mask by FN])


--создание списка таблиц и колонок для маскировки данных в них по соответствующим шаблонам масок
drop table if exists dbo.[Data Masking Setup]

select *
into dbo.[Data Masking Setup]
from (values
	(1,'Customers','Title',5),
	(1,'Customers','Birthday',2),
	(1,'Customers','Comments',3),
	(1,'Customers','Pasport',6),
	(1,'Customers','INN',9),
	(1,'Customers','OGRN',10),
	(1,'Customers','CardCode',11),
	(3,'Customer Contacts','Website',8),
	(3,'Customer Contacts','Address',1),
	(3,'Customer Contacts','Phone No_',7),
	(3,'Customer Contacts','E-Mail',4),

	(8,'Consultants','FirstName',5),
	(8,'Consultants','Pasport',6),
	(8,'Consultants','DateOfBirth',2),
	(8,'Consultants','SSN',9),
	(8,'Consultants','Email',4)
) as t([Table ID], [Table Name], [Field Name], [Template Entry No_])


--функция генерирующая данные для шаблона
go
create or alter function dbo.[fn_string_masking] (
	@string nvarchar(200), -- входная строка
	@type_of_mask int	-- 1) цифры заменяем единицей 2) любые цифры для пин-кода 3) уникальными цифрами не короче исходной длины 
)
returns nvarchar(200)
as
begin

declare
	@int_value int,
	@len int,
	@result nvarchar(200)

if @type_of_mask = 1
begin
	set @result = @string
	while patindex(N'%[02-9]%', @result) > 0
	begin
		set @result = stuff(@result, patindex(N'%[02-9]%', @result), 1, N'1')
	end
	while patindex(N'%[a-zа-я]%', @result) > 0
	begin
		set @result = stuff(@result, patindex(N'%[a-zа-я]%', @result), 1, N'')
	end
end
else if @type_of_mask = 2
begin
	set @len = len(@string)
	set @result = right(abs(checksum(@string)),@len)
end
else if @type_of_mask = 3
begin
	set @int_value = abs(checksum(hashbytes('md5',@string)))
	set @len = len(@string) - len(@int_value)
	set @result = right(concat(left(replicate('', @len) + @string, @len), @int_value),len(@string))
	while patindex(N'%[a-zа-я]%', @result) > 0
	begin
		set @result = stuff(@result, patindex(N'%[a-zа-я]%', @result), 1, N'')
	end
end
return @result
end
go


--последовательность для добавления к маске в случае если значения должны быть уникальными
drop sequence if exists dbo.[sq_for_masking]

create sequence dbo.[sq_for_masking] as
	bigint
	start with 1
	increment by 1
	minvalue 1
	no maxvalue
	no cycle
	cache 1000
go


--создание таблицы логирования работы процедуры
if object_id('dbo.[data_masking_log]','U') is null
begin
	create table dbo.[data_masking_log] (
		[Id] int identity primary key,
		[Datetime] datetime default getdate(),
		[Session Num] int default @@spid,
		[Step Name] nvarchar(200),
		[Step Value] nvarchar(max),
		index ix1 nonclustered ([Session Num])
	)
end
go


--процедура маскирования
create or alter proc dbo.[sp_data_masking]
	@rows_to_update int = 4000, --размер пачки строк для маскирования за итерацию
	@table_id_list varchar(max) = '', --список ИД таблиц (из таблицы [Data Masking Setup]) для маскирования, разделитель |
	@debug bit = 0, --если 1 то только вывода отладочной информации
	@delay_sec smallint = 1, --задержка в секундах между итерациями
	@rowlock bit = 1, --включить хинт
	@use_dbts binary(8) = 0x0 --можно задать максимально учитываемый timestamp
as
set nocount, xact_abort on

if @@servername like '%PROD%'
	throw 50000, 'НА ПРОДЕ НЕ ЗАПУСКАТЬ !', 1

if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values ('процедура', 'СТАРТ')

declare
	@query varchar(max),
	@query_set varchar(max),
	@query_select varchar(max),
	@query_where varchar(max),
	@query_on varchar(max),
	@tbl nvarchar(100),
	@error nvarchar(max)	

drop table if exists #tables
create table #tables (
	name_in_app nvarchar(100),
	name_in_sql nvarchar(100)
)
insert into #tables (name_in_app, name_in_sql)
select distinct
	t.[Table Name],
	s.TABLE_NAME
from
	dbo.[Data Masking Setup] as t
	inner join INFORMATION_SCHEMA.TABLES as s (nolock)
		on s.TABLE_TYPE = 'BASE TABLE'
		and s.TABLE_NAME = t.[Table Name]
	inner join string_split(@table_id_list,'|') as ss
		on (ss.[value] = t.[Table ID]
			or ss.[value] = '')

if @debug = 1 select '#tables', * from #tables

drop table if exists #sql_tables_and_fields
create table #sql_tables_and_fields (
	tbl nvarchar(100),
	fld nvarchar(100),
	tpl int
)
insert into #sql_tables_and_fields (tbl, fld, tpl)
select distinct
	t.name_in_sql,
	s.COLUMN_NAME,
	tf.[Template Entry No_]
from
	dbo.[Data Masking Setup] as tf
	inner join #tables as t
		on tf.[Table Name] = t.name_in_app
	inner join INFORMATION_SCHEMA.COLUMNS as s (nolock)
		on s.TABLE_NAME = t.name_in_sql
		and s.TABLE_SCHEMA = 'dbo'
		and s.COLUMN_NAME = tf.[Field Name]

if @debug = 1 select '#sql_tables_and_fields', * from #sql_tables_and_fields

drop table if exists #errors
create table #errors (
	tbl nvarchar(100),
	msg nvarchar(4000)
)

if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values (concat('таблицы для маскировки - (',(select count(distinct tbl) from #sql_tables_and_fields),')'), (select distinct concat(tbl,' | ') from #sql_tables_and_fields for xml path('')))

declare curs cursor local for
	select distinct	tbl
	from #sql_tables_and_fields
open curs
fetch next from curs into @tbl
while @@fetch_status = 0
begin

	select
		@query = '',
		@query_set = '',
		@query_select = '',
		@query_where = '',
		@query_on = ''

	select
		@query_set = case
						when
							t.is_in_PK is not null
							and m.[Mask by FN] = 0
						then
							concat(@query_set,'t.[',f.fld,'] = ','''',m.[Mask],'-''+','cast(next value for dbo.[sq_for_masking] as varchar)',',',char(13))
						when
							m.[Mask by FN] > 0
						then
							concat(@query_set,'t.[',f.fld,'] = iif(t.[',f.fld,'] = @empty,t.[',f.fld,'],dbo.[fn_string_masking](t.[',f.fld,'],',m.[Mask by FN],'))',',',char(13))
						else
							concat(@query_set,'t.[',f.fld,'] = iif(t.[',f.fld,'] = @empty,t.[',f.fld,'],''',m.[Mask],''')',',',char(13))
						end,
		@query_where = case 
						when
							m.[Mask] = ''
						then
							concat(@query_where,'(t.[',f.fld,'] <> @empty ) or ')
						when
							m.[Mask by FN] = 1
						then
							concat(@query_where,'(t.[',f.fld,'] <> dbo.[fn_string_masking](t.[',f.fld,'],',m.[Mask by FN],') and t.[',f.fld,'] <> @empty ) or ')
						when
							m.[Mask by FN] in (2,3)
						then
							concat(@query_where,'(t.[',f.fld,'] <> @empty and t.[timestamp] < @dbts) or ')
						else
							concat(@query_where,'(t.[',f.fld,'] not like ''',m.[Mask],'%'' and t.[',f.fld,'] <> @empty ) or ')
						end
	from
		#sql_tables_and_fields as f
		inner join dbo.[Data Masking Template] as m
			on f.tpl = m.[Entry No_]
		left join (
			select
				ccu.COLUMN_NAME as is_in_PK,
				tc.TABLE_NAME
			from
				INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc (nolock)
				inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as ccu (nolock)
					on tc.TABLE_NAME = ccu.TABLE_NAME
				inner join INFORMATION_SCHEMA.COLUMNS as cl (nolock)
					on cl.TABLE_NAME = tc.TABLE_NAME
					and ccu.COLUMN_NAME = cl.COLUMN_NAME
			where
				CONSTRAINT_TYPE = 'PRIMARY KEY'
		) as t
			on t.TABLE_NAME = f.tbl
			and t.is_in_PK = f.fld
	where
		f.tbl = @tbl
	
	select
		@query_select = concat(@query_select,'t.[',ccu.COLUMN_NAME,'],'),
		@query_on = concat(@query_on,'t.[',ccu.COLUMN_NAME,'] = s.[',ccu.COLUMN_NAME,'] and ')
	from
		INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc
		inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as ccu
			on tc.TABLE_NAME = ccu.TABLE_NAME
	where
		tc.TABLE_NAME = @tbl
		and CONSTRAINT_TYPE = 'PRIMARY KEY'
	
	select
		@query_set = left(@query_set, len(@query_set) - 2),
		@query_where = left(@query_where, len(@query_where) - 3),
		@query_select = left(@query_select, len(@query_select) - 1),
		@query_on = left(@query_on, len(@query_on) - 4)
	
	set @query = '
		declare
			@empty varchar(1) = '''',
			@rows_to_update int = '+cast(@rows_to_update as varchar)+',
			@row_cnt int,
			@error nvarchar(max),
			@dbts rowversion = ' + iif(@use_dbts = 0x0,'@@dbts',convert(varchar,@use_dbts,1)) + '
	
		insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values (''@@dbts'', convert(varchar, cast(@dbts as binary(8)), 1))
	
		while 1=1
		begin
		'
	
	if exists (
		select
			ccu.COLUMN_NAME
		from
			INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc (nolock)
			inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as ccu (nolock)
				on tc.TABLE_NAME = ccu.TABLE_NAME
			inner join INFORMATION_SCHEMA.COLUMNS as cl (nolock)
				on cl.TABLE_NAME = tc.TABLE_NAME
				and ccu.COLUMN_NAME = cl.COLUMN_NAME
			inner join #sql_tables_and_fields as t
				on t.tbl = tc.TABLE_NAME
				and t.fld = ccu.COLUMN_NAME
		where
			tc.TABLE_NAME = @tbl
			and CONSTRAINT_TYPE = 'PRIMARY KEY'
	)
	begin
		set @query += '
			;with cte as (
			select top(@rows_to_update)
				'+ @query_select +'
			from
				dbo.['+ @tbl +'] as t
			where
				('+ @query_where +')
				' + iif(@use_dbts = 0x0,'','and t.[timestamp] < @dbts') + '
			)	
			update t set
				'+ @query_set +'
			from
				dbo.['+ @tbl +'] as t '+ iif(@rowlock = 1, 'with (rowlock)','') +'
				inner join cte as s
					on '+ @query_on +'
		'
	end
	else
	begin
		set @query += '
			update top(@rows_to_update) t set
				'+ @query_set +'
			from
				dbo.['+ @tbl +'] as t '+ iif(@rowlock = 1, 'with (rowlock)','') +'
			where
				('+ @query_where +')
				' + iif(@use_dbts = 0x0,'','and t.[timestamp] < @dbts') + '
		'
	end
	
	set @query += '
			set @row_cnt = @@rowcount
			
			insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values (''@@rowcount'', @row_cnt)
	
			if @row_cnt < @rows_to_update
				break
			
			waitfor delay '''+cast(timefromparts(0,@delay_sec/60,@delay_sec%60,0,0) as varchar(8))+'''
			
		end
	'
	
	if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values (@tbl, 'будет маскирована')
	
	begin try
		if @debug = 1
			print @query
		else
			exec (@query)
	end try
	begin catch
		set @error = error_message()
		insert into #errors (tbl, msg)
		values (
			@tbl,
			@error
		)
	
		if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values (@tbl, @query)
		if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values ('ошибка', @error)
		
	end catch
	
	fetch next from curs into @tbl

end

close curs

deallocate curs

if (select count(*) from #errors) > 0
	select * from #errors

if @debug = 0 insert into dbo.[data_masking_log] ([Step Name], [Step Value]) values ('процедура', 'СТОП')

go