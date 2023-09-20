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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestNoisyAggregations
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    // There is a type issue with the default expectedQueryRunner H2QueryRunner
    // doing averages as ints instead of floats,
    // e.g., it returns 3.0 for `SELECT avg(linenumber) FROM lineitem` which should be 3.004270876609888
    // This override is to make sure that both queryRunner and expectedQueryRunner are the same type of query runner.
    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testNoisyCountGaussianZeroNoiseScaleVsNormalCount()
    {
        assertQuery("SELECT noisy_count_gaussian(1, 0) FROM lineitem", "SELECT count(*) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(linenumber, 0) FROM lineitem", "SELECT count(linenumber) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(orderkey, 0) FROM lineitem", "SELECT count(orderkey) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(quantity, 0) FROM lineitem", "SELECT count(quantity) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(linestatus, 0) FROM lineitem", "SELECT count(linestatus) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(shipdate, 0) FROM lineitem", "SELECT count(shipdate) FROM lineitem");
    }

    @Test
    public void testNoisyCountGaussianZeroNoiseScaleRandomSeedVsNormalCount()
    {
        assertQuery("SELECT noisy_count_gaussian(1, 0, 10) FROM lineitem", "SELECT count(*) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(linenumber, 0, 10) FROM lineitem", "SELECT count(linenumber) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(orderkey, 0, 10) FROM lineitem", "SELECT count(orderkey) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(quantity, 0, 10) FROM lineitem", "SELECT count(quantity) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(linestatus, 0, 10) FROM lineitem", "SELECT count(linestatus) FROM lineitem");
        assertQuery("SELECT noisy_count_gaussian(shipdate, 0, 10) FROM lineitem", "SELECT count(shipdate) FROM lineitem");
    }

    @Test
    public void testNoisySumGaussianZeroNoiseScaleVsNormalSum()
    {
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(1, 0) FROM lineitem", "SELECT sum(1.0) FROM lineitem");
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(linenumber, 0) FROM lineitem", "SELECT sum(linenumber) FROM lineitem"); // BIGINT
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(quantity, 0) FROM lineitem", "SELECT sum(quantity) FROM lineitem"); // DOUBLE
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(nationkey, 0) FROM nation", "SELECT sum(nationkey) FROM nation"); // INTEGER
    }

    @Test
    public void testNoisySumGaussianZeroNoiseScaleRandomSeedVsNormalCount()
    {
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(1, 0, 10) FROM lineitem", "SELECT sum(1.0) FROM lineitem");
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(linenumber, 0, 10) FROM lineitem", "SELECT sum(linenumber) FROM lineitem"); // BIGINT
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(quantity, 0, 10) FROM lineitem", "SELECT sum(quantity) FROM lineitem"); // DOUBLE
        assertQueryWithSingleDoubleRow("SELECT noisy_sum_gaussian(nationkey, 0, 10) FROM nation", "SELECT sum(nationkey) FROM nation"); // INTEGER
    }

    @Test
    public void testNoisyAvgGaussianZeroNoiseScaleVsNormalAvg()
    {
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(1, 0) FROM lineitem", "SELECT avg(1) FROM lineitem");
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(linenumber, 0) FROM lineitem", "SELECT avg(linenumber) FROM lineitem"); // BIGINT
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(quantity, 0) FROM lineitem", "SELECT avg(quantity) FROM lineitem"); // DOUBLE
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(nationkey, 0) FROM nation", "SELECT avg(nationkey) FROM nation"); // INTEGER
    }

    @Test
    public void testNoisyAvgGaussianZeroNoiseScaleRandomSeedVsNormalCount()
    {
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(1, 0, 10) FROM lineitem", "SELECT avg(1) FROM lineitem");
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(linenumber, 0, 10) FROM lineitem", "SELECT avg(linenumber) FROM lineitem"); // BIGINT
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(quantity, 0, 10) FROM lineitem", "SELECT avg(quantity) FROM lineitem"); // DOUBLE
        assertQueryWithSingleDoubleRow("SELECT noisy_avg_gaussian(nationkey, 0, 10) FROM nation", "SELECT avg(nationkey) FROM nation"); // INTEGER
    }

    private void assertQueryWithSingleDoubleRow(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        MaterializedResult actualResult = computeActual(actual);
        MaterializedResult expectedResult = computeExpected(expected, actualResult.getTypes());

        assertEquals(actualResult.getRowCount(), 1);
        assertEquals(expectedResult.getRowCount(), 1);

        double actualValue = Double.parseDouble(actualResult.getMaterializedRows().get(0).getField(0).toString());
        double expectedValue = Double.parseDouble(expectedResult.getMaterializedRows().get(0).getField(0).toString());

        assertTrue(Math.abs(actualValue - expectedValue) <= 1e-12);
    }
}
