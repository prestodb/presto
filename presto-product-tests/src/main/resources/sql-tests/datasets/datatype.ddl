-- type: hive
CREATE EXTERNAL TABLE %NAME%(
  c_bigint bigint,
  c_double double,
  c_string string,
  c_date date,
  c_timestamp timestamp,
  c_boolean boolean,
  c_short_decimal decimal(5,2),
  c_long_decimal decimal(30,10)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '%LOCATION%'
TBLPROPERTIES('serialization.null.format'='#')
