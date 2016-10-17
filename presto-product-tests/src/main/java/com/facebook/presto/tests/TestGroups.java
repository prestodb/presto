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
    public static final String QUARANTINE = "quarantine";
    public static final String FUNCTIONS = "functions";
    public static final String CLI = "cli";
    public static final String HIVE_CONNECTOR = "hive_connector";
    public static final String SYSTEM_CONNECTOR = "system";
    public static final String JMX_CONNECTOR = "jmx";
    public static final String BLACKHOLE_CONNECTOR = "blackhole";
    public static final String SMOKE = "smoke";
    public static final String JDBC = "jdbc";
    public static final String SIMBA_JDBC = "simba_jdbc";
    public static final String QUERY_ENGINE = "qe";
    public static final String COMPARISON = "comparison";
    public static final String LOGICAL = "logical";
    public static final String SET_OPERATION = "set_operation";
    public static final String JSON_FUNCTIONS = "json_functions";
    public static final String URL_FUNCTIONS = "url_functions";
    public static final String ARRAY_FUNCTIONS = "array_functions";
    public static final String BINARY_FUNCTIONS = "binary_functions";
    public static final String CONVERSION_FUNCTIONS = "conversion_functions";
    public static final String HOROLOGY_FUNCTIONS = "horology_functions";
    public static final String MAP_FUNCTIONS = "map_functions";
    public static final String REGEX_FUNCTIONS = "regex_functions";
    public static final String STRING_FUNCTIONS = "string_functions";
    public static final String MATH_FUNCTIONS = "math_functions";
    public static final String STORAGE_FORMATS = "storage_formats";
    public static final String PROFILE_SPECIFIC_TESTS = "profile_specific_tests";
    public static final String HDFS_IMPERSONATION = "hdfs_impersonation";
    public static final String HDFS_NO_IMPERSONATION = "hdfs_no_impersonation";
    public static final String BASIC_SQL = "basic_sql";
    public static final String AUTHORIZATION = "authorization";
    public static final String POST_HIVE_1_0_1 = "post_hive_1_0_1";
    public static final String HIVE_COERCION = "hive_coercion";

    private TestGroups() {}
}
