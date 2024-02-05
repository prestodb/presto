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

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.DEFAULT_TEST_STANDARD_DEVIATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.avg;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.notEqualDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.withinSomeStdAssertion;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisyAvgGaussianDoubleAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_avg_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testNoisyAvgGaussianDoubleDefinitions()
    {
        getFunction(DOUBLE, DOUBLE); // (col, noiseScale)
        getFunction(DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
        getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE); // (col, noiseScale, lower, upper)
        getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE, BIGINT); // (col, noiseScale, lower, upper, randomSeed)
    }

    // Test DOUBLE noiseScale < 0
    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianDoubleInvalidNoiseScale()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, true);
        double expected = avg(values);
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale) with noiseScale < 0 which means errors",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(-123.0, numRows)),
                expected);
    }

    // Test DOUBLE noiseScale == 0
    @Test
    public void testNoisyAvgGaussianDoubleZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, true);
        double expected = avg(values);
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, true);
        double expected = avg(values);
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale) with noiseScale=0 and 1 null row which means no noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test DOUBLE noiseScale > 0
    @Test
    public void testNoisyAvgGaussianDoubleSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, true);
        double expected = avg(values);
        assertAggregation(
                function,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale) with noiseScale > 0 which means some noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE);

        int numRows = 1000;
        List<Double> values = createTestValues(numRows, false, 1.0, true);
        double expected = avg(values);
        assertAggregation(
                function,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(double, noiseScale) within some std from mean",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    // Test DOUBLE vs. normal AVG
    @Test
    public void testNoisyAvgGaussianDoubleNoiseScaleVsNormalAvg()
    {
        // Test DOUBLE AVG(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DOUBLE);
        String query1 = String.format("SELECT AVG(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test DOUBLE with clipping
    @Test
    public void testNoisyAvgGaussianDoubleClippingZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 4.7; // first value 0 is clipped to 2
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyAvgGaussianDoubleClippingInvalidBound()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, false);
        double lower = 2.0;
        double upper = -8.0;
        double expected = 4.5;
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper) with clipping lower > upper ",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleClippingZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5; // 45 / 9
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper) with noiseScale=0 which means no noise, and clipping, with null values",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleClippingSomeNoiseScale()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5; // 45 / 9
        assertAggregation(
                function,
                notEqualDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper) with noiseScale > 0 which means some noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleClippingSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, false);
        double lower = 2.0;
        double upper = 8.0;
        double expected = 5;
        assertAggregation(
                function,
                withinSomeStdAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper) within some std from mean",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows)),
                expected);
    }

    // Test DOUBLE with clipping and randomSeed
    @Test
    public void testNoisyAvgGaussianDoubleClippingRandomSeed()
    {
        // Test DOUBLE with clipping
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, false, 1.0, false);
        double lower = 2.0;
        double upper = 5.0;
        double expected = 3.8 + 10.4961467597545; // 10.4961467597545 is from noiseScale=12 and randomSeed=10
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, lower, upper, randomSeed)",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(lower, numRows),
                        createRLEBlock(upper, numRows),
                        createRLEBlock(10, numRows)),
                expected);
    }

    // Test DOUBLE with randomSeed
    @Test
    public void testNoisyAvgGaussianDoubleZeroNoiseScaleZeroRandomSeed()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, false);
        double expected = avg(values);
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                expected);
    }

    @Test
    public void testNoisyAvgGaussianDoubleSomeNoiseScaleFixedRandomSeed()
    {
        JavaAggregationFunctionImplementation function = getFunction(DOUBLE, DOUBLE, BIGINT);

        int numRows = 10;
        List<Double> values = createTestValues(numRows, true, 1.0, false);
        assertAggregation(
                function,
                equalDoubleAssertion,
                "Test noisy_avg_gaussian(double, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createDoublesBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                15.496146759754); // 10.4961467597545 is from noiseScale=12 and randomSeed=10
    }

    // Test DOUBLE 0-row input returns NULL
    @Test
    public void testNoisyAvgGaussianDoubleNoInputRowsWithoutGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DOUBLE);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false";

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 1);
        assertNull(actualRows.get(0).getField(0));
    }

    @Test
    public void testNoisyAvgGaussianDoubleNoInputRowsWithGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BIGINT,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.DOUBLE);
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
