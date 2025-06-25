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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestNativeSidecarPlanChecker
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
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setCoordinatorSidecarEnabled(true)
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8089"))
                .build();
        setupNativeSidecarPlugin(queryRunner);
        queryRunner.getCoordinator().createCatalog("hive2", "hive");
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder().build();
    }

    @Test
    public void createAndInsertUnbucketedTable()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT orderkey key1, comment value1 FROM orders", tableName), 15000);
            assertUpdate(format("INSERT INTO %s SELECT orderkey key1, comment value1 FROM orders", tableName), 15000);
        }
        finally {
            // Clean up the temporary tables
            getExpectedQueryRunner().execute(getSession(), format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
        }
    }

    @Test
    public void selectFromUnbucketedTable()
    {
        assertQuery(format("SELECT orderkey key1, comment value1 FROM orders"));
    }
}
