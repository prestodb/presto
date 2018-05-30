-- database: presto; groups: orderby
select * from (select cast(null as bigint) union all select 1) T order by 1 desc nulls first
