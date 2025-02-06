# Presto Opt plus plugin
## Deploy plugin

Step 1. Build the plugin artifact, gets built along with the presto build 

Or

```commandline
./mvnw clean install -DskipTests -pl presto-optplus
```

Step 2. Pick the built zip artifact

```commandline
unzip ./presto-optplus/target/presto-optplus-0.294-SNAPSHOT.zip -d $PRESTO_INSTALL/plugin
```
Step 3. Set the following configurations:

 * etc/password-authenticator.properties (Only needed for local testing, not required for wxd deployment)

```properties
password-authenticator.name=OPTPLUS_PASSWORD_AUTHENTICATOR
```

 * etc/query-rewriter.properties

```properties
optplus.jdbc.coordinator_host=localhost
optplus.jdbc.coordinator_port=8080
optplus.enable_fallback=true
optplus.ssl.jdbc.enable=false
optplus.db2.jdbc_url=jdbc:db2://dummy.ibm.com:12333/dml:user=?;password=?;
optplus.show-rewritten-query=false
```
if JDBC SSL is enabled.

```properties
optplus.jdbc.coordinator_host=localhost
optplus.jdbc.coordinator_port=8080
optplus.enable_fallback=true
optplus.ssl.jdbc.enable=true
optplus.db2.jdbc_url=jdbc:db2://dummy.ibm.com:12333/dml:user=?;password=?;
optplus.show-rewritten-query=false
optplus.ssl.jdbc.trust_store_password=<pass>
optplus.ssl.jdbc.trust_store_path=<path to truststore>
optplus.username=user
optplus.password=pass
```

Step 4. Set session variables
```commandline
set session is_query_rewriter_plugin_enabled=true;
```

### Additional configuration details for WxD deployment of OPTplus plugin

Create `etc/query-rewriter.properties`

1. `optplus.jdbc.coordinator_host` and  `optplus.jdbc.coordinator_port` is set to the `PRESTO_SERVER`'s host and port values,
   same as `COORDINATOR_HOST` and `COORDINATOR_PORT` in the previous versions.
2. `optplus.enable_fallback=true`
3. `optplus.show-rewritten-query=false`
4. `optplus.ssl.jdbc.enable=true`
5. `optplus.db2.jdbc_url=jdbc:db2://dummy.ibm.com:12333/dml:user=?;password=?;` This JDBC URL is to be configured instead of
   of setting environment variable `wxd_optimizer_url`.
6. Previously the value of truststore path was hard coded in the code as `/mnt/infra/tls/lh-ssl-ts.jks`, now there is
   a property that needs to be configured e.g. `optplus.ssl.jdbc.trust_store_path=/mnt/infra/tls/lh-ssl-ts.jks`.
7. Previously truststore key was loaded via a config file, now it can be set on the optplus config
   `optplus.ssl.jdbc.trust_store_password=TRUSTSTORE_KEY_VALUE`
8. `optplus.username` and `optplus.password` are to be loaded from a special location, this is from a special
   user created just for the purpose of optplus and has read access on everything. The details of this service id (aka optplus user) is 
    available on issue : https://github.ibm.com/lakehouse/tracker/issues/16505 and the PRs associated.
    1. Go to the directory pointed by `INTERNAL_SVC_AUTH` environment variable.
    2. Read file `internal_svc_auth_password` (i.e. `cat $INTERNAL_SVC_AUTH/internal_svc_auth_password`) for the value of password and set it as `optplus.password`.
    3. Read file `internal_svc_auth_user` (i.e. `cat $INTERNAL_SVC_AUTH/internal_svc_auth_user`) for the value of username and set it as `optplus.username`.
    Above steps are taken from https://github.ibm.com/lakehouse/tracker/issues/16505, please refer to the original issue for 
   for latest update.
   
If the optmizer plus support needs to be enabled by default, then this session flag needs to be enabled
```
is_query_rewriter_plugin_enabled=true;
```
Same can be enabled by the end user from SQL prompt as `set session is_query_rewriter_plugin_enabled=true;`

## Run tests

First update the properties file

```properties
db2.jdbc_url=jdbc:db2://loud30.almaden.ibm.com:?/dml:user=?;password=?;
db2.tpch_schema=TPCH_SF10000_DEFAULTS
db2.tpch_catalog=iceberg
# hive test data with tpch tables is generated here.
test.data_dir=/tmp/data
test.generate_result_files=false
# Setting it false, causes each test to regenerate data.
test.erase_data_dir=false
```

Run all tests

./mvnw clean test -pl presto-optplus

Run specific test:

./mvnw clean test -Dtest='com.facebook.presto.rewriter.<suitename>'

It is also possible to pass test properties via arguments (useful in test automation scripts)
e.g.

```
./mvnw test -pl presto-optplus -Ddb2.jdbc_url='jdbc:db2://n.ibm.com:12331/?:user=?;password=?;'
```

Following end-to-end tests suites exist:

1. [TestOptPlusTPCHAgainstPrestoResults.java](src/test/java/com/facebook/presto/rewriter/TestOptPlusTPCHAgainstPrestoResults.java) : 
    Each TPCH query is run (with OPT+) and results are compared against the results obtained from running against presto without Opt+.
2. [TestOptPlusFallbackMechanism.java](src/test/java/com/facebook/presto/rewriter/TestOptPlusFallbackMechanism.java): Test to ensure should an Opt+
    query fail we should be able to fallback to presto.
3. [TestOptPlusPassThroughMechanism.java](src/test/java/com/facebook/presto/rewriter/TestOptPlusPassThroughMechanism.java):
    Query pass through tests.

Troubleshooting:

1. Errors such as `Caused by: com.facebook.presto.sql.analyzer.SemanticException: Table iceberg.tpch_sf10000_defaults.customer does not exist
` can be solved by either manually erasing the data dir set in property: `test.data_dir` or setting the flag `test.erase_data_dir` to true.

