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

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestMapTopNFunctions
        extends AbstractTestQueryFramework
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
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .setExtraProperties(ImmutableMap.of("inline-sql-functions", "false"))
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
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setExtraProperties(ImmutableMap.of("inline-sql-functions", "true"))
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testMapTopNValues()
    {
        assertQuery("SELECT TRY(map_top_n_values(c0, BIGINT '6455219767830808341')) FROM (VALUES (MAP(ARRAY[1, 2], ARRAY[ARRAY[1, 3], ARRAY[1, 4]]))) t(c0)");
        assertQuery("SELECT TRY(map_top_n_values(c0, BIGINT '2')) FROM (VALUES (MAP(ARRAY[1, 2, 3], ARRAY[ARRAY[1, 3], null, ARRAY[1, 4]]))) t(c0)");

        // The transform lambda form is only available through the native sidecar. Without the sidecar, the coordinator
        // resolves the Presto comparator form function(V, V, int) instead.
        if (sidecarEnabled) {
            String input = "WITH input(c0) AS ( VALUES (MAP(ARRAY[3,2,5,4,1], ARRAY[30,10,50,20,40])), (MAP(ARRAY[3,2,1], ARRAY[10,30,20])), (MAP(ARRAY[2,1], ARRAY[10,20])) ) ";
            assertQuery(input + "SELECT map_top_n_values(c0, 3, (k, v) -> v) FROM input",
                    input + "SELECT map_top_n_values(c0, 3, (x, y) -> CAST(IF(x < y, -1, IF(x = y, 0, 1)) AS INTEGER)) FROM input");
            assertQuery(input + "SELECT map_top_n_values(c0, 3, (k, v) -> -v) FROM input",
                    input + "SELECT map_top_n_values(c0, 3, (x, y) -> CAST(IF(x > y, -1, IF(x = y, 0, 1)) AS INTEGER)) FROM input");
            assertQuery(input + "SELECT map_top_n_values(c0, 3, (k, v) -> k) FROM input",
                    input + "SELECT transform(map_top_n_keys(c0, 3), k -> element_at(c0, k)) FROM input");
            assertQuery("WITH input(c0) AS ( VALUES (MAP(ARRAY[1,2,3], ARRAY[CAST(-2147483648 AS INTEGER), CAST(0 AS INTEGER), CAST(2147483647 AS INTEGER)])) ) SELECT map_top_n_values(c0, 2, (k, v) -> v) FROM input",
                    "WITH input(c0) AS ( VALUES (MAP(ARRAY[1,2,3], ARRAY[CAST(-2147483648 AS INTEGER), CAST(0 AS INTEGER), CAST(2147483647 AS INTEGER)])) ) SELECT map_top_n_values(c0, 2, (x, y) -> CAST(IF(x < y, -1, IF(x = y, 0, 1)) AS INTEGER)) FROM input");

            // A NULL lambda behaves like the two-argument form.
            assertQuery(input + "SELECT map_top_n_values(c0, 3, NULL) FROM input",
                    input + "SELECT map_top_n_values(c0, 3) FROM input");

            // A comparator lambda binds to the transform signature and is evaluated over (key, value), so it returns a
            // different result than Presto. Presto returns [0] for this query.
            assertQuery("SELECT map_top_n_values(MAP(ARRAY[1, 2, 3], ARRAY[0, 5, 3]), 1, (x, y) -> IF(x < y, 1, IF(x > y, -1, 0)))", "SELECT ARRAY[5]");
        }

        assertQueryFails(
                "SELECT map_top_n_values(c0, BIGINT '6455219767830808341') FROM (VALUES (MAP(ARRAY[1, 2], ARRAY[ARRAY[null, null], ARRAY[1, 2]]))) t(c0)",
                ".* Ordering nulls is not supported Top-level Expression: (native|presto)\\.default\\.map_top_n_values\\(field, 6455219767830808341:BIGINT\\)");
    }
}
