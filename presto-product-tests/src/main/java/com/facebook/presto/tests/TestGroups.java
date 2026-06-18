/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests;

public final class TestGroups
{
    public static final String CREATE_TABLE = "create_table";
    public static final String CREATE_DROP_VIEW = "create_drop_view";
    public static final String ALTER_TABLE = "alter_table";
    public static final String SIMPLE = "simple";
    public static final String FUNCTIONS = "functions";
    public static final String CLI = "cli";
    public static final String SYSTEM_CONNECTOR = "system";
    public static final String JMX_CONNECTOR = "jmx";
    public static final String BLACKHOLE_CONNECTOR = "blackhole";
    public static final String SMOKE = "smoke";
    public static final String JDBC = "jdbc";
    public static final String MYSQL = "mysql";
    public static final String MYSQL_MIXED_CASE = "mysql_mixed_case";
    public static final String PRESTO_JDBC = "presto_jdbc";
    public static final String SIMBA_JDBC = "simba_jdbc";
    public static final String QUERY_ENGINE = "qe";
    public static final String COMPARISON = "comparison";
    public static final String LOGICAL = "logical";
    public static final String JSON_FUNCTIONS = "json_functions";
    public static final String STORAGE_FORMATS = "storage_formats";
    public static final String PROFILE_SPECIFIC_TESTS = "profile_specific_tests";
    public static final String HDFS_IMPERSONATION = "hdfs_impersonation";
    public static final String HDFS_NO_IMPERSONATION = "hdfs_no_impersonation";
    public static final String HIVE_PARTITIONING = "hive_partitioning";
    public static final String AUTHORIZATION = "authorization";
    public static final String POST_HIVE_1_0_1 = "post_hive_1_0_1";
    public static final String HIVE_COERCION = "hive_coercion";
    public static final String CASSANDRA = "cassandra";
    public static final String SQL_SERVER = "sqlserver";
    public static final String LDAP = "ldap";
    public static final String LDAP_CLI = "ldap_cli";
    public static final String SKIP_ON_CDH = "skip_on_cdh";
    public static final String TLS = "tls";
    public static final String ROLES = "roles";
    public static final String CANCEL_QUERY = "cancel_query";
    public static final String BIG_QUERY = "big_query";
    public static final String HIVE_TABLE_STATISTICS = "hive_table_statistics";
    public static final String KAFKA = "kafka";
    public static final String AVRO = "avro";
    public static final String ICEBERG = "iceberg";
    public static final String HIVE_LIST_CACHING = "hive_list_caching";
    public static final String INVALIDATE_METASTORE_CACHE = "invalidate_metastore_cache";
    public static final String MIXED_CASE = "mixed_case";
    public static final String HIVE_SYMLINK_TABLE = "hive_symlink_table";

    private TestGroups() {}
}
