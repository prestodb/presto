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
    public void testMapTopNKeys()
    {
        assertQuery("SELECT TRY(map_top_n_keys(c0, BIGINT '6455219767830808341')) FROM (VALUES (MAP(ARRAY[1, 2], ARRAY[ARRAY[1, null], ARRAY[1, null]]))) t(c0)");
        assertQuery("SELECT TRY(map_top_n_keys(c0, BIGINT '6455219767830808341')) FROM (VALUES (MAP(ARRAY[1, 2], ARRAY[ARRAY[null, null], ARRAY[1, 2]]))) t(c0)");

        assertQuery("WITH input(c0) AS ( VALUES (MAP(ARRAY[3,2,5,4,1], ARRAY[1,1,1,1,1])), (MAP(ARRAY[3,2,1], ARRAY[1,1,1])), (MAP(ARRAY[2,1], ARRAY[1,1])) ) SELECT map_top_n_keys(c0, 3, (x,y) -> x) AS result FROM input",
                "WITH input(c0) AS ( VALUES (MAP(ARRAY[3,2,5,4,1], ARRAY[1,1,1,1,1])), (MAP(ARRAY[3,2,1], ARRAY[1,1,1])), (MAP(ARRAY[2,1], ARRAY[1,1])) ) SELECT map_top_n_keys(c0, 3, (x,y) -> CAST(IF(x < y, -1, IF(x = y, 0, 1)) AS INTEGER)) AS result FROM input");
        assertQuery("WITH input(c0) AS ( VALUES (MAP(ARRAY[1,2,3,4,5], ARRAY[1,1,1,1,1])) ) SELECT map_top_n_keys(c0, 3, (x, y) -> -x) AS result FROM input",
                "WITH input(c0) AS ( VALUES (MAP(ARRAY[1,2,3,4,5], ARRAY[1,1,1,1,1])) ) SELECT map_top_n_keys(c0, 3, (x, y) -> CAST(IF(x > y, -1, IF(x = y, 0, 1)) AS INTEGER)) AS result FROM input");
        assertQuery("WITH input(c0) AS ( VALUES (MAP(ARRAY[CAST(-2147483648 AS INTEGER), CAST(0 AS INTEGER), CAST(2147483647 AS INTEGER)], ARRAY[1,1,1])) ) SELECT map_top_n_keys(c0, 2, (x, y) -> x) AS result FROM input",
                "WITH input(c0) AS ( VALUES (MAP(ARRAY[CAST(-2147483648 AS INTEGER), CAST(0 AS INTEGER), CAST(2147483647 AS INTEGER)], ARRAY[1,1,1])) ) SELECT map_top_n_keys(c0, 2, (x, y) -> CAST(IF(x < y, -1, IF(x = y, 0, 1)) AS INTEGER)) AS result FROM input");
    }
}
