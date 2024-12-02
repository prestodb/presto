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
package com.facebook.presto.sidecar.typemanager;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;

@Test(singleThreaded = true)
public class TestNativeSidecarPluginTypeManager
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
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunner(true, true);
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testTypes()
    {
        assertQuery("SELECT substr(comment, 1, 10), length(comment), trim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), ltrim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), rtrim(comment) FROM orders");
        assertQuery("select lower(comment) from nation");
        assertQuery("select array[nationkey], array_constructor(comment) from nation");
        assertQuery("SELECT nationkey, bit_count(nationkey, 10) FROM nation ORDER BY 1");
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK%'");
        assertQueryFails(
                "SELECT trim(comment, ' ns'), ltrim(comment, 'a b c'), rtrim(comment, 'l y') FROM orders",
                "java.lang.IllegalArgumentException: Unknown type CodePoints");
        assertQueryFails("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK%' escape '#'",
                "java.lang.IllegalArgumentException: Unknown type LikePattern");
    }
}
