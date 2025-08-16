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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.util.RetryAnalyzer;
import com.facebook.presto.util.RetryCount;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createTypedLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.notEqualDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.sumLong;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisySumGaussianLongAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_sum_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    private static final double DEFAULT_TEST_STANDARD_DEVIATION = 1.0;

    @Test
    public void testNoisySumGaussianLongDefinitions()
    {
        getFunction(TINYINT, DOUBLE); // (col, noiseScale)
        getFunction(TINYINT, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(TINYINT, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(TINYINT, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)

        getFunction(SMALLINT, DOUBLE); // (col, noiseScale)
        getFunction(SMALLINT, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(SMALLINT, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(SMALLINT, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)

        getFunction(INTEGER, DOUBLE); // (col, noiseScale)
        getFunction(INTEGER, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(INTEGER, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(INTEGER, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)

        getFunction(BIGINT, DOUBLE); // (col, noiseScale)
        getFunction(BIGINT, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)
    }

    // Test TINYINT type, noiseScale == 0
    @Test
    public void testNoisySumGaussianTinyIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(TINYINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(tinyint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(TINYINT, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test SMALLINT type, noiseScale == 0
    @Test
    public void testNoisySumGaussianSmallIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(SMALLINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(smallint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(SMALLINT, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test INTEGER type, noiseScale == 0
    @Test
    public void testNoisySumGaussianIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(INTEGER, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(integer, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(INTEGER, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test TINYINT vs. normal SUM
    @Test
    public void testNoisySumGaussianTinyIntNoiseScaleVsNormalSum()
    {
        // Test SUM(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.TINYINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.TINYINT);
        String query1 = String.format("SELECT SUM(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test SMALLINT vs. normal SUM
    @Test
    public void testNoisySumGaussianSmallIntNoiseScaleVsNormalSum()
    {
        // Test SUM(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.SMALLINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.SMALLINT);
        String query1 = String.format("SELECT SUM(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test INTEGER vs. normal SUM
    @Test
    public void testNoisySumGaussianIntegerNoiseScaleVsNormalSum()
    {
        // Test SUM(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.INTEGER,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.INTEGER);
        String query1 = String.format("SELECT SUM(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test LONG noiseScale < 0
    @Test(expectedExceptions = PrestoException.class)
    public void testNoisySumGaussianLongInvalidNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale) with noiseScale < 0 which means errors",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(-123.0, numRows)),
                expected);
    }

    // Test BIGINT type, noiseScale == 0
    @Test
    public void testNoisySumGaussianLongZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    @Test
    public void testNoisySumGaussianLongZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, true);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale) with noiseScale=0 and 1 null row which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test DOUBLE noiseScale > 0
    @Test
    public void testNoisySumGaussianLongSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                notEqualDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale) with noiseScale > 0 which means some noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    @Test
    public void testNoisySumGaussianLongSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE);

        BiFunction<Object, Object, Boolean> withinSomeStdAssertion = (actual, expected) -> {
            double actualValue = new Double(actual.toString());
            double expectedValue = new Double(expected.toString());
            return expectedValue - 50 * DEFAULT_TEST_STANDARD_DEVIATION <= actualValue && actualValue <= expectedValue + 50 * DEFAULT_TEST_STANDARD_DEVIATION;
        };

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                withinSomeStdAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale) within some std from mean",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    // Test BIGINT vs. normal SUM
    @Test
    public void testNoisySumGaussianLongNoiseScaleVsNormalSum()
    {
        // Test SUM(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query1 = String.format("SELECT SUM(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test BIGINT with clipping
    @Test
    public void testNoisySumGaussianLongClippingZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        long expected = 47;
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testNoisySumGaussianLongClippingInvalidBound()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = -8.0;
        double expected = 47;
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper) with clipping lower > upper ",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisySumGaussianLongClippingZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 45;
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping, with null values",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisySumGaussianLongClippingSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 45;
        assertAggregation(
                noisySumGaussian,
                notEqualDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper) with noiseScale > 0 which means some noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test(retryAnalyzer = RetryAnalyzer.class)
    @RetryCount(100)
    public void testNoisySumGaussianLongClippingSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        BiFunction<Object, Object, Boolean> withinSomeStdDoubleAssertion = (actual, expected) -> {
            double actualValue = new Double(actual.toString());
            double expectedValue = new Double(expected.toString());
            return expectedValue - 5 * DEFAULT_TEST_STANDARD_DEVIATION <= actualValue && actualValue <= expectedValue + 5 * DEFAULT_TEST_STANDARD_DEVIATION;
        };

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 45;
        assertAggregation(
                noisySumGaussian,
                withinSomeStdDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper) within some std from mean",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    // Test BIGINT with clipping and randomSeed
    @Test
    public void testNoisySumGaussianLongClippingRandomSeed()
    {
        // Test with clipping
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 5.0;
        double expected = 48.4961467597545;
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, lower, upper, randomSeed)",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows),
                        createRLEBlock(10, numRows)),
                expected);
    }

    // Test BIGINT with randomSeed
    @Test
    public void testNoisySumGaussianLongZeroNoiseScaleZeroRandomSeed()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double expected = sumLong(values);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                expected);
    }

    @Test
    public void testNoisySumGaussianLongSomeNoiseScaleFixedRandomSeed()
    {
        JavaAggregationFunctionImplementation noisySumGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        assertAggregation(
                noisySumGaussian,
                equalDoubleAssertion,
                "Test noisy_sum_gaussian(bigint, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                55.496146759754); // x + 10 is when true sum = x, noiseScale=12 and randomSeed=10
    }

    // Test LONG 0-row input returns NULL
    @Test
    public void testNoisySumGaussianLongNoInputRowsWithoutGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false";

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 1);
        assertNull(actualRows.get(0).getField(0));
    }

    @Test
    public void testNoisySumGaussianLongNoInputRowsWithGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false GROUP BY " + columnName;

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 0);
    }

    private List<MaterializedRow> runQuery(String query)
    {
        LocalQueryRunner runner = new LocalQueryRunner(session);

        MaterializedResult actualResults = runner.execute(query).toTestTypes();
        return actualResults.getMaterializedRows();
    }

    private JavaAggregationFunctionImplementation getFunction(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(
                FUNCTION_AND_TYPE_MANAGER.lookupFunction(FUNCTION_NAME, fromTypes(arguments)));
    }
}
