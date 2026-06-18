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
package com.facebook.presto.benchmark.framework;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestBenchmarkRunnerConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(BenchmarkRunnerConfig.class)
                .setTestId(null)
                .setBenchmarkSuiteSupplier("mysql")
                .setEventClients("json")
                .setJsonEventLogFile(null)
                .setContinueOnFailure(false)
                .setMaxConcurrency(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("test-id", "12345")
                .put("benchmark-suite-supplier", "custom-supplier")
                .put("event-clients", "human-readable")
                .put("json.log-file", "verifier-json.log")
                .put("continue-on-failure", "true")
                .put("max-concurrency", "70")
                .build();

        BenchmarkRunnerConfig expected = new BenchmarkRunnerConfig()
                .setTestId("12345")
                .setBenchmarkSuiteSupplier("custom-supplier")
                .setEventClients("human-readable")
                .setJsonEventLogFile("verifier-json.log")
                .setContinueOnFailure(true)
                .setMaxConcurrency(70);

        assertFullMapping(properties, expected);
    }
}
