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

import com.facebook.presto.nativetests.NativeTestsUtils;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCudfSidecarPlugin
        extends AbstractTestQueryFramework
{
    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(true)
                .setBuiltInWorkerFunctionsEnabled(true)
                .setEnableCudf(true)
                .build();

        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables("PARQUET");
    }

    @Test
    public void testCudfSessionPropertiesAvailable()
    {
        String propertyName = "cudf_allow_cpu_fallback";
        Optional<PropertyMetadata<?>> propertyMetadata = getQueryRunner()
                .getMetadata()
                .getSessionPropertyManager()
                .getSystemSessionPropertyMetadata(propertyName);
        assertTrue(propertyMetadata.isPresent(), "Expected cuDF session property metadata to be registered");
        String defaultValue = String.valueOf(propertyMetadata.get().getDefaultValue());
        assertEquals(defaultValue, "true", "Default matches CUDF enablement");

        assertQuerySucceeds("SET SESSION cudf_allow_cpu_fallback = true");
        // TODO: Fix error and reenable test: AstExpressionUtils.h:440
        // Unsupported expression by AST: native.default.like(field, cudf_allow_cpu_fallback:VARCHAR)
        // MaterializedResult showResult = getQueryRunner()
        //         .execute("SHOW SESSION LIKE 'cudf_allow_cpu_fallback'");
        // MaterializedRow row = showResult.getMaterializedRows().get(0);
        // assertEquals(row.getField(0), propertyName, "Session property name should match");
        // assertEquals(row.getField(1), "true", "Session property value should be set to true");
    }

    @Test
    public void testCudfFunctionRegistration()
    {
        String cudfFunctionName = "native.default.cardinality";
        List<SqlFunction> functions = getQueryRunner()
                .getMetadata()
                .getFunctionAndTypeManager()
                .listFunctions(getSession(), Optional.empty(), Optional.empty());

        boolean functionRegistered = functions.stream()
                .anyMatch(function -> function.getSignature().getName().toString().equals(cudfFunctionName));
        assertTrue(functionRegistered, "cuDF function should be registered when CUDF is enabled");

        String nonCudfFunctionName = "native.default.khyperloglog_agg";
        functions = getQueryRunner()
                .getMetadata()
                .getFunctionAndTypeManager()
                .listFunctions(getSession(), Optional.empty(), Optional.empty());

        functionRegistered = functions.stream()
                .anyMatch(function -> function.getSignature().getName().toString().equals(nonCudfFunctionName));
        assertFalse(functionRegistered, "Non-cuDF function should not be registered when CUDF is enabled");
    }

    @Test
    public void testBasicQueryExecution()
    {
        Object minNationKey = computeScalar("SELECT min(nationkey) FROM nation");
        assertEquals(((Number) minNationKey).longValue(), 0L, "Nation table should be available with data");
    }
}
