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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.CreationException;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertTrue;

public class TestSourceQueryModule
{
    private static final Map<String, String> DEFAULT_CONFIGURATION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("test-id", "test")
            .build();

    @Test
    public void testMySqlSourceQuerySupplier()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new SourceQueryModule(ImmutableSet.of()))
                        .build());
        Injector injector = app
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                        .put("source-query.database", "jdbc://localhost:1080")
                        .put("source-query.suites", "test")
                        .put("source-query.max-queries-per-suite", "1000")
                        .build())
                .initialize();
        assertTrue(injector.getInstance(SourceQuerySupplier.class) instanceof MySqlSourceQuerySupplier);
    }

    @Test
    public void testCustomSourceQuerySupplier()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new SourceQueryModule(ImmutableSet.of("test-supplier")))
                        .build());
        app.setRequiredConfigurationProperties(
                ImmutableMap.<String, String>builder()
                        .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                        .put("source-query.supplier", "test-supplier")
                        .build()).initialize();
    }

    @Test(expectedExceptions = CreationException.class, expectedExceptionsMessageRegExp = "Unable to create injector.*")
    public void testInvalidSourceQuerySupplier()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new SourceQueryModule(ImmutableSet.of("test-supplier")))
                        .build());
        app.setRequiredConfigurationProperties(
                ImmutableMap.<String, String>builder()
                        .putAll(DEFAULT_CONFIGURATION_PROPERTIES)
                        .put("source-query.supplier", "unknown-supplier")
                        .build()).initialize();
    }
}
