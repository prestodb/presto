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
package com.facebook.presto.nativetests;

import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestSqlInvokedFunctions;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestSqlInvokedFunctions
        extends AbstractTestSqlInvokedFunctions
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setExtraProperties(ImmutableMap.of("inline-sql-functions", "true"))
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        else {
            queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        }
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    /// Default implementation of Sql invoked function `map_keys_by_top_n_values` is not compatible with Velox. Sidecar
    /// should be enabled to use the native compatible Sql invoked function from `native-sql-invoked-functions-plugin`.
    @Override
    @Test
    public void testTry()
    {
        if (sidecarEnabled) {
            super.testTry();
        }
        else {
            @Language("RegExp") String arraySortLambdaUnsupported = ".*array_sort with comparator lambda that cannot be rewritten into a transform is not supported.*";

            assertQueryFails("SELECT\n" +
                    "    TRY(map_keys_by_top_n_values(c0, BIGINT '6455219767830808341'))\n" +
                    "FROM (\n" +
                    "    VALUES\n" +
                    "        MAP(\n" +
                    "            ARRAY[1, 2], ARRAY[\n" +
                    "                ARRAY[1, null],\n" +
                    "                ARRAY[1, null]\n" +
                    "            ]\n" +
                    "        )\n" +
                    ") t(c0)", arraySortLambdaUnsupported);

            assertQueryFails("SELECT\n" +
                    "    TRY(map_keys_by_top_n_values(c0, BIGINT '6455219767830808341'))\n" +
                    "FROM (\n" +
                    "    VALUES\n" +
                    "        MAP(\n" +
                    "            ARRAY[1, 2], ARRAY[\n" +
                    "                ARRAY[null, null],\n" +
                    "                ARRAY[1, 2]\n" +
                    "            ]\n" +
                    "        )\n" +
                    ") t(c0)", arraySortLambdaUnsupported);

            // Test try with array method with an input array containing null values.
            // the error should be suppressed and just return null.
            assertQuery("SELECT TRY(ARRAY_MAX(ARRAY [ARRAY[1, NULL], ARRAY[1, 2]]))", "SELECT NULL");
        }
    }
}
