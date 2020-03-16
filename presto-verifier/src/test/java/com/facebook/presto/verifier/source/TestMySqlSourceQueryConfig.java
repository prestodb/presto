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
package com.facebook.presto.verifier.source;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestMySqlSourceQueryConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(MySqlSourceQueryConfig.class)
                .setDatabase(null)
                .setTableName("verifier_queries")
                .setSuites("")
                .setMaxQueriesPerSuite(100_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("database", "jdbc://127.0.0.1:8001")
                .put("table-name", "query_source")
                .put("suites", "test1,test2")
                .put("max-queries-per-suite", "50")
                .build();
        MySqlSourceQueryConfig expected = new MySqlSourceQueryConfig()
                .setDatabase("jdbc://127.0.0.1:8001")
                .setTableName("query_source")
                .setSuites("test1,test2")
                .setMaxQueriesPerSuite(50);

        assertFullMapping(properties, expected);
    }
}
