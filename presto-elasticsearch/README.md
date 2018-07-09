# presto-elasticsearch
presto connector for elasticsearch


## Requirements

* Mac OS X or Linux
* Presto 0.203
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)
* elasticsearch 2.4.0
## Building

Presto-elasticsearch is a standard Maven project. Simply run the following command from the project root directory:

    ./mvn clean package -DskipTests

## intall

es 2.x
```
connector.name=elasticsearch

elasticsearch.cluster.name=ideal-bigdata
elasticsearch.transport.hosts=localhost:9300
```

es 5.x
```
connector.name=elasticsearch5

elasticsearch.cluster.name=ideal-bigdata
elasticsearch.transport.hosts=localhost:9300
```

es 6.x
```
connector.name=elasticsearch6

elasticsearch.cluster.name=ideal-bigdata
elasticsearch.transport.hosts=localhost:9300
```
look: Cannot be used simultaneously

## SQL Usage

```SELECT * FROM test1 WHERE age >30 AND city = 'world'```

```desc test1```


## Beyond SQL

* Search

select * from test1 where  _name = match_query('Lucy')

* Search match_phrase

select * from test1 where  _name = match_phrase('Lucy')

* Strong pushDown

select * from test1 where _dsl = '{"query":{"match":{"city":{"query":"world"}}}}' and _name = match_query('Lucy')

### pushDown
+ _ field is an extension field
Use the _ field and it will be pushed down to es for search

+ -dsl field will be fully pushed down for custom dsl

## session parameter

| Name                                    | Default        | Type    | Description
| -------------------------------------   | -------------- | ------- | ----------------------------------------------------------------------------------
| es6.optimize_split_shards_enabled       | true           | boolean | Set to true to split non-indexed queries by shards splits. Should generally be
| es6.scroll_search_batch_size            | 100            | integer | max of 100 hits will be returned for each scroll. Default 100                 
| es6.scroll_search_timeout               | 60000          | bigint  | If set, will enable scrolling of the search request for the specified timeout


demo:```set session es6.scroll_search_batch_size=150```

## Does not support features
+ insert into(Already supported)
+ create table as (Already supported)
+ create table (Already supported)
+ drop table(Already supported)
+ update
+ delete
