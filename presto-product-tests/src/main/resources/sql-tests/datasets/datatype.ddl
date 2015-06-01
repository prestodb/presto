-- type: hive
CREATE EXTERNAL TABLE %NAME%(
  c_bigint bigint,
  c_double double,
  c_varchar varchar(100),
  c_date date,
  c_timestamp timestamp,
  c_boolean boolean
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '%LOCATION%'
TBLPROPERTIES('serialization.null.format'='#')
