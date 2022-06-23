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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.JdbcQueryRunner.createJdbcQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestJdbcIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createJdbcQueryRunner(ORDERS);
    }

    @Test
    public void testPlanOptimizerFilterPushdownCorrectness()
    {
        assertUpdate("CREATE TABLE test_pushdown1(col1 bigint, col2 bigint)");
        assertUpdate("INSERT INTO test_pushdown1 VALUES (5, 4), (2, 4), (4, 4)", "VALUES(3)");
        // PickTableLayout#pushPredicateIntoTableScan cannot push down this predicate however, JdbcPlanOptimizer can.
        assertQuery("SELECT * FROM test_pushdown1 WHERE col1 + col2 = 8", "VALUES(4, 4)");
    }

    @Test
    public void testPlanOptimizerAndDefaultFilterPushdownCorrectness()
    {
        assertUpdate("CREATE TABLE test_pushdown2(col1 bigint, col2 bigint)");
        assertUpdate("INSERT INTO test_pushdown2 VALUES (5, 4), (2, 4), (4, 4)", "VALUES(3)");
       // PickTableLayout#pushPredicateIntoTableScan and JdbcPlanOptimizer are both capable of pushing down these predicates.
        assertQuery("SELECT * FROM test_pushdown2 WHERE col1 = 5 AND col2 = 4", "VALUES(5, 4)");
    }

    @Test
    public void testIncompatibleFilterPushdownCorrectness()
    {
        assertUpdate("CREATE TABLE test_pushdown3(col1 bigint, col2 bigint)");
        assertUpdate("INSERT INTO test_pushdown3 VALUES (5, 4), (2, 4), (4, 4)", "VALUES(3)");
        // Neither PickTableLayout#pushPredicateIntoTableScan or JdbcPlanOptimizer are capable of pushing down these predicates.
        assertQuery("SELECT * FROM test_pushdown3 WHERE NOT(col1 + col2 <= 6)", "VALUES (5, 4), (4, 4)");
    }

    @Test
    public void testArithmetic()
    {
        assertUpdate("CREATE TABLE test_arithmetic(col1 bigint, col2 bigint)");
        assertUpdate("INSERT INTO test_arithmetic VALUES (5, 4), (2, 4), (4, 4)", "VALUES(3)");
        assertQuery("SELECT * FROM test_arithmetic WHERE NOT(col1 + col2 - 6 = 0)", "VALUES (5, 4), (4, 4)");
    }

    @Test
    public void testBooleanNot()
    {
        assertUpdate("CREATE TABLE test_not(col1 bigint, col2 boolean)");
        assertUpdate("INSERT INTO test_not VALUES (5, true), (2, true), (4, false)", "VALUES(3)");
        assertQuery("SELECT col1 FROM test_not WHERE NOT(col2)", "VALUES(4)");
    }
}
