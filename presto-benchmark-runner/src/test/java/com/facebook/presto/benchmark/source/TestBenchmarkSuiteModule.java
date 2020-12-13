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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.CreationException;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertTrue;

public class TestBenchmarkSuiteModule
{
    private static final Map<String, String> DEFAULT_CONFIGURATION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("test-id", "test")
            .build();

    @Test
    public void testDbBenchmarkSuiteSupplier()
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new BenchmarkSuiteModule(ImmutableSet.of()))
                .build());
        Injector injector = null;
        try {
            injector = app
                    .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                            .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                            .put("benchmark-suite.database-url", "jdbc://localhost:1080")
                            .put("benchmark-suite.suite", "test")
                            .build())
                    .initialize();
            assertTrue(injector.getInstance(BenchmarkSuiteSupplier.class) instanceof MySqlBenchmarkSuiteSupplier);
        }
        finally {
            if (injector != null) {
                injector.getInstance(LifeCycleManager.class).stop();
            }
        }
    }

    @Test
    public void testCustomBenchmarkSuiteSupplier()
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new BenchmarkSuiteModule(ImmutableSet.of("test-supplier")))
                .build());
        Injector injector = null;
        try {
            injector = app
                    .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                            .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                            .put("benchmark-suite-supplier", "test-supplier")
                            .build())
                    .initialize();
        }
        finally {
            if (injector != null) {
                injector.getInstance(LifeCycleManager.class).stop();
            }
        }
    }

    @Test(expectedExceptions = CreationException.class, expectedExceptionsMessageRegExp = "Unable to create injector.*")
    public void testInvalidBenchmarkSuiteSupplier()
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new BenchmarkSuiteModule(ImmutableSet.of("test-supplier")))
                .build());
        Injector injector = null;
        try {
            injector = app
                    .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                            .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                            .put("benchmark-query-supplier", "unknown-supplier")
                            .build())
                    .initialize();
        }
        finally {
            if (injector != null) {
                injector.getInstance(LifeCycleManager.class).stop();
            }
        }
    }
}
