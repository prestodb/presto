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
package com.facebook.presto.accumulo;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;

import static org.testng.Assert.assertEquals;

public class TestAccumuloIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestAccumuloIntegrationSmokeTest()
            throws Exception
    {
        super(AccumuloQueryRunner.createAccumuloQueryRunner(ImmutableMap.of(), true));
    }

    @AfterClass
    public void cleanup()
    {
        AccumuloQueryRunner.dropTpchTables(queryRunner, getSession());
    }

    @Override
    public void testDescribeTable()
            throws Exception
    {
        // Override base class because table descriptions for Accumulo connector include comments
        MaterializedResult actual = computeActual("DESC ORDERS").toJdbcTypes();
        assertEquals("orderkey", actual.getMaterializedRows().get(0).getField(0));
        assertEquals("bigint", actual.getMaterializedRows().get(0).getField(1));
        assertEquals("custkey", actual.getMaterializedRows().get(1).getField(0));
        assertEquals("bigint", actual.getMaterializedRows().get(1).getField(1));
        assertEquals("orderstatus", actual.getMaterializedRows().get(2).getField(0));
        assertEquals("varchar", actual.getMaterializedRows().get(2).getField(1));
        assertEquals("totalprice", actual.getMaterializedRows().get(3).getField(0));
        assertEquals("double", actual.getMaterializedRows().get(3).getField(1));
        assertEquals("orderdate", actual.getMaterializedRows().get(4).getField(0));
        assertEquals("date", actual.getMaterializedRows().get(4).getField(1));
        assertEquals("orderpriority", actual.getMaterializedRows().get(5).getField(0));
        assertEquals("varchar", actual.getMaterializedRows().get(5).getField(1));
        assertEquals("clerk", actual.getMaterializedRows().get(6).getField(0));
        assertEquals("varchar", actual.getMaterializedRows().get(6).getField(1));
        assertEquals("shippriority", actual.getMaterializedRows().get(7).getField(0));
        assertEquals("integer", actual.getMaterializedRows().get(7).getField(1));
        assertEquals("comment", actual.getMaterializedRows().get(8).getField(0));
        assertEquals("varchar", actual.getMaterializedRows().get(8).getField(1));
    }
}
