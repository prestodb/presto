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
package com.facebook.presto.benchmark.source;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestBenchmarkSuiteConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(BenchmarkSuiteConfig.class)
                .setSuite(null)
                .setSuitesTableName("benchmark_suites")
                .setQueriesTableName("benchmark_queries"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("suite", "test1")
                .put("suites-table-name", "suite_source")
                .put("queries-table-name", "query_source")
                .build();
        BenchmarkSuiteConfig expected = new BenchmarkSuiteConfig()
                .setSuite("test1")
                .setSuitesTableName("suite_source")
                .setQueriesTableName("query_source");

        assertFullMapping(properties, expected);
    }
}
