-- database: presto; tables: table_with_header, table_with_footer, table_with_header_and_footer; groups: hive, hive_file_header;
--! name: simple_scan with header
SELECT count(*) FROM table_with_header
--!
34816
--! name: filter with header
SELECT *
FROM table_with_header
WHERE data = 'data'
--!
--! name: simple_scan with footer
SELECT count(*) FROM table_with_footer
--!
34816
--! name: filter with footer
SELECT *
FROM table_with_footer
WHERE data = 'data'
--!
--! name: simple_scan with header and footer
SELECT count(*) FROM table_with_header_and_footer
--!
34816
--! name: filter with header and footer
SELECT *
FROM table_with_header_and_footer
WHERE data = 'data'
--!
