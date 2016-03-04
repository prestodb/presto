-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select 5 * c_bigint, c_double + 15, c_string, c_date, c_timestamp, c_boolean, cast(c_short_decimal / 2 as decimal(5,2)), try_cast(c_long_decimal % 10 as decimal(30,10)) from datatype;
select * from ${mutableTables.hive.datatype}
--!
60|27.25|String1|1999-01-08|1999-01-08 02:05:06|true|061.61|00000000000000000000.0123456789|
125|70.52|test|1952-01-05|1989-01-08 04:05:06|false|160.61|-00000000000000000000.0123456789|
4820|15.245|Again|1936-02-08|2005-01-09 04:05:06|false|166.91|00000000000000000000.9876543210|
500|27.25|testing|1949-07-08|2002-01-07 01:05:06|true|-196.61|-00000000000000000000.9876543210|
500|114.8777|AGAIN|1987-04-09|2010-01-02 04:03:06|true|000.00|00000000000000000000.0000000000|
26260|27.25|sample|1987-04-09|2010-01-02 04:03:06|true|061.50|00000000000000000001.0000000000|
500|24.8777|STRING1|1923-04-08|2010-01-02 05:09:06|true|005.01|00000000000000000002.0000000000|
44980|113.8777|again|1987-04-09|2010-01-02 04:03:06|false|000.00|00000000000000000009.9999999999|
500|27.8788|string1|1922-04-02|2010-01-02 02:05:06|true|500.00|-00000000000000000009.9999999999|
28740|82.87|sample|1987-04-06|2010-01-02 04:03:06|true|-499.99|00000000000000000000.0000000001|
28740|82.87|Sample|1987-04-06|2010-01-02 04:03:06|true|090.59|-00000000000000000000.0000000001|
28740|82.87|sample|1987-04-06|2010-01-02 04:03:06|true|090.59|00000000000000000000.0123456789|
28740|82.87|sample|1987-04-06|2010-01-02 04:03:06|true|090.59|00000000000000000000.0123456789|
25000|82.87|testing|null|2010-01-02 04:03:06|null|null|null|
30000|null|null|1987-04-06|null|true|null|null|
null|113.52|null|null|null|true|090.59|null|
