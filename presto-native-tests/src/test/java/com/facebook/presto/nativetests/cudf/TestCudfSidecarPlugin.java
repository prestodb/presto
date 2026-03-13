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
package com.facebook.presto.nativetests.cudf;

import com.facebook.presto.Session;
import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.sidecar.NativeSidecarPlugin;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestCudfSidecarPlugin
        extends AbstractTestQueryFramework
{
    private final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .addExtraProperties(ImmutableMap.of(
                        "coordinator-sidecar-enabled", "true",
                        "exclude-invalid-worker-session-properties", "true"))
                .setBuiltInWorkerFunctionsEnabled(true)
                .build();

        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        queryRunner.loadSessionPropertyProvider(
                NativeSystemSessionPropertyProviderFactory.NAME,
                ImmutableMap.of());
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    @Test
    public void testCudfSessionPropertyProvider()
    {
        // Verify CUDF session property is available
        String propertyName = "cudf_allow_cpu_fallback";
        Optional<PropertyMetadata<?>> propertyMetadata = getQueryRunner()
                .getMetadata()
                .getSessionPropertyManager()
                .getSystemSessionPropertyMetadata(propertyName);
        assertTrue(propertyMetadata.isPresent(), "Expected cuDF session property metadata to be registered");
        String defaultValue = String.valueOf(propertyMetadata.get().getDefaultValue());
        assertEquals(defaultValue, "true", "Default matches CUDF enablement");

        assertQuerySucceeds("SET SESSION cudf_allow_cpu_fallback = true");
        MaterializedResult showResult = getQueryRunner()
                .execute("SHOW SESSION LIKE 'cudf_allow_cpu_fallback'");
        MaterializedRow row = showResult.getMaterializedRows().get(0);
        assertEquals(row.getField(0), propertyName, "Session property name should match");
        assertEquals(row.getField(1), "true", "Session property value should be set to true");

        // Verify that non-CUDF session properties are not available with native sidecar
        String nonCudfProperty = "legacy_array_agg";
        Optional<PropertyMetadata<?>> nonCudfPropertyMetadata = getQueryRunner()
                .getMetadata()
                .getSessionPropertyManager()
                .getSystemSessionPropertyMetadata(nonCudfProperty);
        assertFalse(nonCudfPropertyMetadata.isPresent(), "Expected non-CUDF session property to NOT be available");

        // Verify that setting a non-CUDF session property fails
        assertQueryFails("SET SESSION legacy_array_agg = true", ".*Session property.*does not exist.*");

        // Verify that session properties flow through to Velox config with proper validation
        // When cudf_memory_percent is set to 0, it should trigger a validation error
        Session failingSession = Session.builder(getSession())
                .setSystemProperty("cudf_memory_percent", "0")
                .setSystemProperty("cudf_allow_cpu_fallback", "false")
                .build();

        try {
            computeActual(
                    failingSession,
                    "select min(nationkey) from nation");
            fail("Expected query to fail when cudf_memory_percent is zero");
        }
        catch (RuntimeException e) {
            assertTrue(
                    e.getMessage().matches("(?s).*cudf\\.memory_percent.*greater than 0.*"),
                    "Expected cudf.memory_percent validation to propagate to cuDF config");
        }

        // Verify that queries succeed with valid default session properties
        assertQuerySucceeds("select min(nationkey) from nation");
    }
}
