-- type: hive
CREATE %EXTERNAL% TABLE %NAME%
(
  id INT,
  data STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '%LOCATION%'
TBLPROPERTIES(
    "skip.header.line.count"="2",
    "skip.footer.line.count"="2"
)
