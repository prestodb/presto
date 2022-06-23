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
package com.facebook.presto.functionNamespace.mysql;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestMySqlFunctionNamespaceManagerConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(MySqlFunctionNamespaceManagerConfig.class)
                .setFunctionNamespacesTableName("function_namespaces")
                .setFunctionsTableName("sql_functions")
                .setUserDefinedTypesTableName("user_defined_types"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("function-namespaces-table-name", "sql_function_namespaces")
                .put("functions-table-name", "sql_invoked_functions")
                .put("user-defined-types-table-name", "user_types")
                .build();
        MySqlFunctionNamespaceManagerConfig expected = new MySqlFunctionNamespaceManagerConfig()
                .setFunctionNamespacesTableName("sql_function_namespaces")
                .setFunctionsTableName("sql_invoked_functions")
                .setUserDefinedTypesTableName("user_types");

        assertFullMapping(properties, expected);
    }
}
