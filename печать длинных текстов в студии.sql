--ПРОЦЕДУРА ДЛЯ ВЫВОДА В СТУДИИ ДЛИННЫХ ОПРЕДЕЛЕНИЙ ОБЪЕКТОВ И ПРОЧИХ ТЕКСТОВ

create or alter proc #PrintingLongText
	@text nvarchar(max)
as
set nocount on

declare
	@pos int
	,@sep varchar(5) = ','

set @text = replace(replace(@text, char(13), char(10)), char(10) + char(10), char(10)) + char(10)

while (1=1)
begin
    set @pos = charindex(char(10), @text)

	if (@pos = 0)
	begin
		print @text
		break
	end
	else if (@pos > 4000)
	begin
	    select @text = stuff(@text, max(pos), len(@sep), @sep + char(10))
		from (values 
			(charindex(@sep, @text, 3900))
			,(charindex(@sep, @text, 1500))
			,(charindex(@sep, @text))
		) as t(pos)

		continue
	end

    print left(@text, @pos - 1)
    
	set @text = substring(@text, @pos + 1, len(@text))
end

go
