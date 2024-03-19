--ПРИ ПЕРЕИМЕНОВАНИИ ОБЪЕКТА ЧЕРЕЗ СТУДИЮ ИЛИ SP_RENAME В ОПРЕДЕЛЕНИИ ОСТАЕТСЯ СТАРОЕ ИМЯ (ЛЕЧИТСЯ ЧЕРЕЗ ALTER)

set nocount, xact_abort on
set ansi_warnings off
set tran isolation level read uncommitted

drop table if exists #t
drop table if exists #s

create table #s (
	[имя в системе] varchar(300)
	,[имя в определении] varchar(300)
	,[определение] varchar(300)
	,[имена различаются] as iif([имя в системе] <> [имя в определении], 'ДА', 'НЕТ')
)

select
	object_name(object_id) as nm
	,[definition] as df
into #t
from sys.sql_modules (nolock)

declare
	@t varchar(max)
	,@df varchar(300)
	,@nm sysname

declare curs cursor for
	select nm, left(df, 300), df
	from #t
open curs
fetch next from curs into @nm, @df, @t
while @@fetch_status = 0
begin

--блок извлечения имени из определения
set @t = replace(@t, char(10), char(13)) + char(13)

declare
	@pos1 int = 0
	,@pos2 int = 0
	,@schema sysname = null
	,@name sysname = null

while 1=1
begin
	set @pos1 = charindex('--', @t)
	set @pos2 = charindex('/*', @t)

	if @pos1 = 0 and @pos2 = 0
		break
	else if (@pos1 < @pos2 and @pos1 > 0) or @pos2 = 0
	begin
		set @pos2 = charindex(char(13), @t, @pos1)
		set @pos2 = iif(@pos2 = 0, len(@t), @pos2 - @pos1 + 1)
		set @t = stuff(@t, @pos1, @pos2, '')
	end
	else if (@pos2 < @pos1 and @pos2 > 0) or @pos1 = 0
	begin
		set @pos1 = charindex('*/', @t, @pos2)
		set @pos1 = iif(@pos1 = 0, len(@t), @pos1 - @pos2 + 2)
		set @t = stuff(@t, @pos2, @pos1, '')
	end	
end

set @t = replace(replace(@t, char(13), ' '), char(9), ' ')
set @t = trim(substring(@t, charindex('create', @t) + 6, len(@t)))

select @pos1 = min(a)
from (values
	(nullif(charindex('[', @t), 0))
	,(nullif(charindex(' ', @t), 0))
	,(nullif(charindex('"', @t), 0))
) as t(a)

set @t = trim(substring(@t, @pos1, len(@t)))

if left(@t, 1) = '.'
	set @t = trim(substring(@t, 2, len(@t)))

while @name is null
begin
	set @pos1 = 2
	
	if left(@t, 1) = '"'
		while 1=1
		begin
			if substring(@t, @pos1, 1) = '"'
				if substring(@t, @pos1 + 1, 1) <> '"'
					break
				else
					set @pos1 += 1
	
			set @pos1 += 1
		end
	else if left(@t, 1) = '['
		set @pos1 = charindex(']', @t, @pos1)
	else
		select @pos1 = min(a) - 1
		from (values
			(nullif(charindex('(', @t, @pos1), 0))
			,(nullif(charindex('.', @t, @pos1), 0))
			,(nullif(charindex(' ', @t, @pos1), 0))
		) as t(a)

	if @schema is not null
		set @name = trim(substring(@t, 1, @pos1))
	else
		if left(trim(substring(@t, @pos1 + 1, len(@t))), 1) = '.'
		begin
			set @schema = trim(substring(@t, 1, @pos1))
			set @t = trim(substring(@t, charindex('.', @t, @pos1 + 1) + 1, len(@t)))
		end
		else
			set @name = trim(substring(@t, 1, @pos1))
end

--print concat(nullif(@schema, '') + '.', @name)

	insert into #s ([имя в системе], [имя в определении], [определение])
	values (@nm, parsename(@name, 1), @df)

	fetch next from curs into @nm, @df, @t
end
close curs
deallocate curs

select * from #s order by [имена различаются]
