-- database: presto; groups: qe, conversion_functions
SELECT TRY_CAST(10 as VARCHAR), TRY_CAST('ala' as BIGINT)
