-- type: hive
CREATE TABLE %NAME%
(
  id_employee INT,
  first_name STRING,
  last_name STRING,
  date_of_employment STRING,
  department INT,
  id_department INT,
  name STRING,
  salary INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
