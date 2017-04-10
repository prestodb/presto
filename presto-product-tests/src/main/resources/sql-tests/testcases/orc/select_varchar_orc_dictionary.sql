-- database: presto; groups: varchar; tables: orc_varchar_dictionary;
--!
SELECT c_varchar FROM orc_varchar_dictionary WHERE c_varchar IS NOT NULL LIMIT 1
--!
-- delimiter: |; trimValues: true; ignoreOrder: true;
column_va
