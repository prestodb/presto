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
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DICTIONARY_AGGREGATION;
import static com.facebook.presto.SystemSessionProperties.REORDER_JOINS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestHeavyHittersAggregation
        extends AbstractTestQueryFramework
{
    public TestHeavyHittersAggregation()
    {
        super(() -> {
            LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                    .setSystemProperty(REORDER_JOINS, "false") // no JOIN reordering
                    .build());

            queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

            return queryRunner;
        });
    }

    @Test
    public void testUsingValues()
    {
        assertQuery(
                "SELECT " +
                        "     approx_heavy_hitters(x, 30) " +
                        "FROM (VALUES '0', '1', '1', '2') t(x)",
                "VALUES (MAP(ARRAY['1'], ARRAY[2]))");

    }

    @Test
    public void testAggregationOverJoin()
    {
        // Join produces DictionaryBlocks so needs special treatment in dictionary aggregation
        assertQuery(
                "SELECT to_hex(checksum(DISTINCT l.comment)) FROM tpch.sf1.lineitem l JOIN tpch.\"sf0.1\".orders USING(orderkey) WHERE orderpriority = '1-URGENT'",
                "VALUES '2D0814DA01053A47'");
    }
}
