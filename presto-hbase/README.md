# presto-hbase-connector
[Prestodb](https://prestodb.io/) connector for apache HBase, it's a lightweight HBase connector which uses Apache Zookeeper to hold the metadata. This connector is based on Hadoop 3.1, HBase 2.0 and Prestodb 0.241. 

## Overview
The Presto HBase connector allows access to HBase data from Presto. This document describes how to setup the Presto HBase connector to run SQL against HBase.
* Note:    HBase 2.0 or above is required.

###  How to build

Presto-hbase is a Maven project. You can package the connector jar file by running the following command:
```
mvn clean install

```


### Catalog config
- Create a *hbase.properties* hbase configuration file under Presto *catalog* directory
- Add configurations for *hbase.properties* as below:
```
connector.name=hbase
hbase.zookeepers=10.1.3.1:2181,10.1.3.2:2181,10.1.3.3:2181
hbase.internal.table.drop.enabled=true
zookeeper.znode.parent=/hbase-secure
java.security.krb5.conf=/etc/krb5.conf
hbase.keytab.file=/etc/security/keytabs/hbase.headless.keytab
hbase.kerberos.principal=hbase-sdp163@HADOOP.COM
hbase.master.kerberos.principal=hbase/_HOST@HADOOP.COM
hbase.regionserver.kerberos.principal=hbase/_HOST@HADOOP.COM


```
### Example of use
- Connect to HBase catalog. 
```
./presto-cli --server 10.1.3.16:8087 --catalog hbase

```
- Select and use the *default* schema, which refers to HBase's default namespace *default*
```
 use default;
```
- Create an example  table *test*

```
CREATE TABLE default.test_yyj_0417 (
  rowkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:info:name,age:info:age,birthday:info:date'
)
```

- Add some data to the table
```
INSERT into test_yyj_0417  VALUES
('1', 'Jack Ma', 35, DATE '1988-11-12' ),
('2', 'Grady Chen', 33, DATE '1989-01-15' ),
('3', 'Bluce Ju', 23, DATE '1982-02-25' ),
('4', 'yyj1', 1, DATE '2024-04-17' ),
('5', 'yyj2', 2, DATE '2024-04-18' ),
('6', 'yyj3', 3, DATE '2024-04-19' ),
('7', 'yyj4', 1, DATE '2024-04-17' ),
('8', 'yyj5', 2, DATE '2024-04-18' ),
('9', 'yyj6', 3, DATE '2024-04-19' );
```

- Create an example table under schema *testns*
```
CREATE TABLE testns.test (
  rowkey VARCHAR,
  name VARCHAR,
  age BIGINT,
  birthday DATE
)
WITH (
  column_mapping = 'name:info:name,age:info:age,birthday:info:date'
)
```
- Drop table *test* under schema *testns*
```
USE testns; //swich to schema testns
DROP TABLE test_yyj_0417; //drop table test_yyj_0417
```
- Drop schema *testns*
   Note: We can only drop the schemas that have no tables.

```
DROP SCHEMA testns;

```