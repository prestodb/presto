# Presto Connector for VictoriaMetrics

## Build
Run `./mvnw assemble` in the project root directory.
The connector files will be in `build/plugin` directory.

Alternatively, you can build one "fat" jar: `./gradlew fatJar`
The connector will be packaged into single .jar file in `build/libs`.
directory.

## Develop
1. Open this directory in IntelliJ IDEA. It will automatically import all 
the dependencies from build.gradle.kts.


## Description

Exports the following table:

```sql
CREATE TABLE metrics (
    account_id uint32, -- used only for cluster version
    project_id uint32, -- used only for cluster version
    name string not null, -- metric name like `{__name__="foo", bar="baz", xx="yy", ...}`
    timestamp int64 not null,
    value float64 not null,
 )
```
Then query:
```
SELECT * FROM metrics 
WHERE REGEXP_LIKE(name, '__name__="<search_for_this_name>"') 
AND REGEXP_LIKE(name, 'foo="<search_for_this_foo_value>"') 
AND timestamp > NOW() - INTERVAL '10 hours'
```

will be converted to request: 
`/api/v1/export?match={__name__=~"sss", foo="xxx"}&start=...&end=....`

### Alternative
```
SELECT name, name.foo, name['foo'], timestamp, value
FROM metrics
WHERE name = 'http_total{foo="abcd"}'
OR (name.__name__ = 'http_total' AND name.foo = 'abcd')
OR (name['__name__'] = 'http_total' AND name['foo'] = 'abcd')
```


### Manual
1. If config `victoriametrics.export-endpoints` ends with `/` then it 
   will be used as is. Otherwise, `/api/v1/export` will be appended.
   If schema is not present, `http` will be used.

   Note that Presto splits tasks per `host:port` pairs, so if you set
   multiple endpoints for one `host:port` pair, then only one random
   of those endpoints will be used for each query. 


### TODO
1. What's the max line length in /export response?
1. What's max num of datapoints in a line (more than 2^31)?
1. Can /export response body NOT end with a newline?
1. I see export values are ints and not doubles, is it as designed?
