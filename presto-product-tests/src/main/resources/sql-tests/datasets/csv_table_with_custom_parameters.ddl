-- type: hive
CREATE %EXTERNAL% TABLE %NAME%
(
	c_bigint BIGINT,
	c_varchar VARCHAR(255))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = "S", "quoteChar" = "Q", "escapeChar" = "E")
STORED AS TEXTFILE
LOCATION '%LOCATION%'
