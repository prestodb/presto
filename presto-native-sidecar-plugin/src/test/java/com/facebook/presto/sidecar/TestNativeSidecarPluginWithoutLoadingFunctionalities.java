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
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;

public class TestNativeSidecarPluginWithoutLoadingFunctionalities
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        // Installing the native sidecar plugin on a native cluster does not load the plugin functionalities because
        // we aren't loading the individual functionalities.
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        // Installing the native sidecar plugin on a Java cluster, does not load the plugin functionalities because
        // we aren't loading the individual functionalities.
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        return queryRunner;
    }

    @Test
    public void testBasicQueries()
    {
        assertQuery("SELECT ARRAY['abc']");
        assertQuery("SELECT ARRAY[1, 2, 3]");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), trim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), rtrim(comment) FROM orders");
        assertQuery("select lower(comment) from nation");
        assertQuery("SELECT mod(orderkey, linenumber) FROM lineitem");
        assertQuery("select corr(nationkey, nationkey) from nation");
        assertQuery("select count(comment) from orders");
        assertQuery("select count(*) from nation");
        assertQuery("select count(abs(orderkey) between 1 and 60000) from orders group by orderkey");
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY GROUPING SETS (())");
    }
}
