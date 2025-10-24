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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.scalar.sql.NativeSqlInvokedFunctionsPlugin;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

public class TestCatalogFilteredFunctionsWithoutSidecar
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(false)
                .build();
        queryRunner.installPlugin(new NativeSqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testHiveCatalogFunctionFailsWithoutSidecar()
    {
        assertQueryFails("SELECT hive.default.initcap('Hello world')", "(?s).*Function hive.default.initcap not registered*");
    }

    @Test
    public void testNativeCatalogFunctionFailsWithoutSidecar()
    {
        assertQueryFails("SELECT native.default.array_sum(ARRAY[1, 2, 3])", "(?s).*Function native.default.array_sum not registered*");
    }

    @Test
    public void testCustomCatalogFunctionFailsWithoutSidecar()
    {
        assertQueryFails("SELECT mycatalog.myschema.custom_func('test')", "(?s).*Function mycatalog.myschema.custom_func not registered*");
    }
}
