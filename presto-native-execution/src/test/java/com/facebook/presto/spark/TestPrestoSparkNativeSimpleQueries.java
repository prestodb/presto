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
package com.facebook.presto.spark;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createEmptyTable;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersHll;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPart;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartSupp;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPrestoBenchTables;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;

public class TestPrestoSparkNativeSimpleQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createOrdersHll(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createRegion(queryRunner);
        createPartitionedNation(queryRunner);
        createBucketedCustomer(queryRunner);
        createCustomer(queryRunner);
        createPart(queryRunner);
        createPartSupp(queryRunner);
        createRegion(queryRunner);
        createSupplier(queryRunner);
        createEmptyTable(queryRunner);
        createPrestoBenchTables(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        return PrestoSparkNativeQueryRunnerUtils.createHiveRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Override
    protected void assertQuery(String sql)
    {
        super.assertQuery(sql);
        PrestoSparkNativeQueryRunnerUtils.assertShuffleMetadata();
    }

    @Test
    public void testMapOnlyQueries()
    {
        assertQuery("SELECT * FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders WHERE orderkey <= 200");
        assertQuery("SELECT nullif(orderkey, custkey) FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders ORDER BY orderkey LIMIT 4");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");
    }

    @Test
    public void testJoins()
    {
        assertQuery("SELECT * FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
    }

    @Test
    public void testFailures()
    {
        assertQueryFails("SELECT orderkey / 0 FROM orders", ".*division by zero.*");
    }

    /**
     * Test native execution of cpp functions declared via a json file.
     * `eq()` Scalar function & `sum()` Aggregate function are defined in `src/test/resources/external_functions.json`
     */
    @Test
    public void testJsonFileBasedFunction()
    {
        assertQuery("SELECT json.test_schema.eq(1, linenumber) FROM lineitem", "SELECT 1 = linenumber FROM lineitem");
        assertQuery("SELECT json.test_schema.sum(linenumber) FROM lineitem", "SELECT sum(linenumber) FROM lineitem");
    }
}
