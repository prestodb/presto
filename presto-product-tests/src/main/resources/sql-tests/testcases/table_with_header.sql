-- database: presto; tables: table_with_header; groups: hive;
--! name: simple_scan
SELECT count(*) FROM table_with_header
--!
34816
--! name: filter
SELECT *
FROM table_with_header
WHERE data = 'data'
--!
