-- type: hive
CREATE %EXTERNAL% TABLE %NAME%
(
  id INT,
  data STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '%LOCATION%'
TBLPROPERTIES("skip.footer.line.count"="2")
