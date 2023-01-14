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
package com.facebook.presto.tests;

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;

public class TestFractionalTableSample
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(TEST_SESSION);
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @org.testng.annotations.Test
    public void testTableSampleWIthF()
    {
        assertQuerySucceeds("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.000000000000000000001)");
        assertQuerySucceeds("SELECT count(*) FROM tpch.tiny.orders TABLESAMPLE BERNOULLI (0.02)");
    }
}
