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
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createTypedLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.DEFAULT_TEST_STANDARD_DEVIATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.avgLong;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.notEqualDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.withinSomeStdAssertion;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisyAvgGaussianLongAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_avg_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testNoisyAvgGaussianLongDefinitions()
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
    public void testNoisyAvgGaussianTinyIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(TINYINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(tinyint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(TINYINT, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test SMALLINT type, noiseScale == 0
    @Test
    public void testNoisyAvgGaussianSmallIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(SMALLINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(smallint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(SMALLINT, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test INTEGER type, noiseScale == 0
    @Test
    public void testNoisyAvgGaussianIntZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(INTEGER, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(integer, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createTypedLongsBlock(INTEGER, values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test TINYINT vs.normal AVG
    @Test
    public void testNoisyAvgGaussianTinyIntNoiseScaleVsNormalAvg()
    {
        // Test AVG(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.TINYINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.TINYINT);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test SMALLINT vs.normal AVG
    @Test
    public void testNoisyAvgGaussianSmallIntNoiseScaleVsNormalAvg()
    {
        // Test AVG(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.SMALLINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.SMALLINT);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test INTEGER vs.normal AVG
    @Test
    public void testNoisyAvgGaussianIntegerNoiseScaleVsNormalAvg()
    {
        // Test AVG(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.INTEGER,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.INTEGER);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test LONG noiseScale < 0
    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianLongInvalidNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale) with noiseScale < 0 which means errors",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(-123.0, numRows)),
                expected);
    }

    // Test BIGINT type, noiseScale == 0
    @Test
    public void testNoisyAvgGaussianLongZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale) with noiseScale=0 and 1 null row which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test DOUBLE noiseScale > 0
    @Test
    public void testNoisyAvgGaussianLongSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale) with noiseScale > 0 which means some noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale) within some std from mean",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    // Test BIGINT vs.normal AVG
    @Test
    public void testNoisyAvgGaussianLongNoiseScaleVsNormalAvg()
    {
        // Test AVG(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test BIGINT with clipping
    @Test
    public void testNoisyAvgGaussianLongClippingZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 4.7;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianLongClippingInvalidBound()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = -8.0;
        double expected = 4.7;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper) with clipping lower > upper ",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongClippingZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5;
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping, with null values",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongClippingSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5;
        assertAggregation(
                noisyAvgGaussian,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper) with noiseScale > 0 which means some noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongClippingSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5;
        assertAggregation(
                noisyAvgGaussian,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper) within some std from mean",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    // Test BIGINT with clipping and randomSeed
    @Test
    public void testNoisyAvgGaussianLongClippingRandomSeed()
    {
        // Test with clipping
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, false, 1L, false);
        double lower = 2.0;
        double upper = 5.0;
        double expected = 3.8 + 10.4961467597545; // 10.4961467597545 is from noiseScale=12 and randomSeed=10
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, lower, upper, randomSeed)",
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
    public void testNoisyAvgGaussianLongZeroNoiseScaleZeroRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double expected = avgLong(values);
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianLongSomeNoiseScaleFixedRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyAvgGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 10;
        List<Long> values = createTestValues(numRows, true, 1L, false);
        double expected = 5 + 10.4961467597545; // 10.4961467597545 is from noiseScale=12 and randomSeed=10
        assertAggregation(
                noisyAvgGaussian,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(bigint, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                expected);
    }

    // Test LONG 0-row input returns NULL
    @Test
    public void testNoisyAvgGaussianLongNoInputRowsWithoutGroupBy()
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
    public void testNoisyAvgGaussianLongNoInputRowsWithGroupBy()
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
