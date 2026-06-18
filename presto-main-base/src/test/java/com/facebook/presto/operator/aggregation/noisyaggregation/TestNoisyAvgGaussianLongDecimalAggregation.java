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
import com.facebook.presto.common.type.DecimalType;
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
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.Decimals.MAX_PRECISION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.DEFAULT_TEST_STANDARD_DEVIATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.avgLong;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.notEqualDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.toNullableStringList;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.withinSomeStdAssertion;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisyAvgGaussianLongDecimalAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_avg_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_PRECISION, 0);

    @Test
    public void testNoisyAvgGaussianDecimalDefinitions()
    {
        getFunction(LONG_DECIMAL_TYPE, DOUBLE); // (col, noiseScale)
        getFunction(LONG_DECIMAL_TYPE, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)
    }

    // Test LONG DECIMAL noiseScale < 0
    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianLongDecimalInvalidNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale) with noiseScale < 0 which means errors",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(-123.0, numRows)),
                expected);
    }

    // Test LONG DECIMAL noiseScale == 0
    @Test
    public void testNoisyAvgGaussianLongDecimalZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale) with noiseScale=0 and 1 null row which means no noise",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test LONG DECIMAL noiseScale > 0
    @Test
    public void testNoisyAvgGaussianLongDecimalSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale) with noiseScale > 0 which means some noise",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale) within some std from mean",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    // Test LONG DECIMAL vs.normal AVG
    @Test
    public void testNoisyAvgGaussianLongDecimalNoiseScaleVsNormalAvg()
    {
        // Test Avg(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DECIMAL);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test LONG DECIMAL with clipping
    @Test
    public void testNoisyAvgGaussianLongDecimalClippingZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 4.7;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianLongDecimalClippingInvalidBound()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = -8.0;
        double expected = 4.5;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper) with clipping lower > upper ",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalClippingZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping, with null values",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalClippingSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 4.5;
        assertAggregation(
                noisyAvgGaussian,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper) with noiseScale > 0 which means some noise",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalClippingSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 4.5;
        assertAggregation(
                noisyAvgGaussian,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper) within some std from mean",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    // Test LONG DECIMAL clipping with random seed
    @Test
    public void testNoisyAvgGaussianLongDecimalClippingRandomSeed()
    {
        // Test with clipping
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 5.0;
        double expected = 3.8 + 10.4961467597545; // 10.4961467597545 is from noiseScale=12 and randomSeed=10
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, lower, upper, randomSeed) that needs to fix sign",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows),
                        createRLEBlock(10, numRows)),
                expected);
    }

    // Test 0-row input returns NULL
    @Test
    public void testNoisyAvgGaussianLongDecimalNoInputRowsWithoutGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DECIMAL);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false";

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 1);
        assertNull(actualRows.get(0).getField(0));
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalNoInputRowsWithGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DECIMAL);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false GROUP BY " + columnName;

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 0);
    }

    // Test LONG DECIMAL with randomSeed
    @Test
    public void testNoisyAvgGaussianLongDecimalZeroNoiseScaleZeroRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongDecimalSomeNoiseScaleFixedRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(LONG_DECIMAL_TYPE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double expected = 5 + 10.4961467597545; // 10.4961467597545 is from noiseScale=12 and randomSeed=10;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(long decimal, noiseScale, randomSeed) with noiseScale=0 which means no noise and a random seed",
                new Page(
                        createLongDecimalsBlock(toNullableStringList(values)),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                expected);
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
