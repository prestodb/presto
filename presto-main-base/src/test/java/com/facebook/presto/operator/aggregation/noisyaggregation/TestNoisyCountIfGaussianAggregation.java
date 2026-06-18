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

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.DEFAULT_TEST_STANDARD_DEVIATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.countTrue;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalDoubleAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.withinSomeStdAssertion;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisyCountIfGaussianAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_count_if_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testNoisyCountIfGaussianDefinitions()
    {
        getFunction(BOOLEAN, DOUBLE); // (col, noiseScale)
        getFunction(BOOLEAN, DOUBLE, BIGINT); // (col, noiseScale, randomSeed)
    }

    // Test BOOLEAN  noiseScale < 0
    @Test(expectedExceptions = PrestoException.class)
    public void testNoisyCountIfGaussianInvalidNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE);

        int numRows = 10;
        List<Boolean> values = createTestValues(numRows, false, true, false);
        double expected = countTrue(values);
        assertAggregation(
                noisyCountIfGaussian,
                equalDoubleAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale) with noiseScale < 0 which means errors",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(-123.0, numRows)),
                expected);
    }

    // Test BOOLEAN  noiseScale == 0
    @Test
    public void testNoisyCountIfGaussianZeroNoiseScale()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE);

        int numRows = 10;
        List<Boolean> values = createTestValues(numRows, false, true, false);
        double expected = countTrue(values);
        assertAggregation(
                noisyCountIfGaussian,
                equalDoubleAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale) with noiseScale=0 which means no noise",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    @Test
    public void testNoisyCountIfGaussianZeroNoiseScaleWithNull()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE);

        int numRows = 10;
        List<Boolean> values = createTestValues(numRows, true, true, false);
        double expected = countTrue(values);
        assertAggregation(
                noisyCountIfGaussian,
                equalDoubleAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale) with noiseScale=0 and 1 null row which means no noise",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(0.0, numRows)),
                expected);
    }

    // Test BOOLEAN  noiseScale > 0
    @Test
    public void testNoisyCountIfGaussianSomeNoiseScaleWithinSomeStd()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE);

        int numRows = 1000;
        List<Boolean> values = createTestValues(numRows, true, true, false);
        double expected = countTrue(values);
        assertAggregation(
                noisyCountIfGaussian,
                withinSomeStdAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale) within some std from mean",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows)),
                expected);
    }

    // Test BOOLEAN vs. normal COUNT_IF
    @Test
    public void testNoisyCountIfGaussianNoiseScaleVsNormalCountIf()
    {
        // Test BOOLEAN  count_if(col) producing the same values

        int numRows = 10;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BOOLEAN,
                StandardTypes.DOUBLE));
        String columnName = buildColumnName(StandardTypes.BOOLEAN);
        String query1 = String.format("SELECT COUNT_IF(%s) FROM %s", columnName, data);
        String query2 = String.format("SELECT %s(%s, %f) FROM %s", FUNCTION_NAME, columnName, 0.0, data);

        List<MaterializedRow> actualRows = runQuery(query1);
        double result1 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        actualRows = runQuery(query2);
        double result2 = Double.parseDouble(actualRows.get(0).getField(0).toString());

        assertEquals(result2, result1);
    }

    // Test BOOLEAN  with randomSeed
    @Test
    public void testNoisyCountIfGaussianZeroNoiseScaleZeroRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE, BIGINT);

        int numRows = 10;
        List<Boolean> values = createTestValues(numRows, true, true, false);
        double expected = countTrue(values);
        assertAggregation(
                noisyCountIfGaussian,
                equalDoubleAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                expected);
    }

    @Test
    public void testNoisyCountIfGaussianSomeNoiseScaleFixedRandomSeed()
    {
        JavaAggregationFunctionImplementation noisyCountIfGaussian = getFunction(BOOLEAN, DOUBLE, BIGINT);

        int numRows = 10;
        List<Boolean> values = createTestValues(numRows, true, true, false);
        double expected = countTrue(values) + 10; // 10.4961467597545 is from noiseScale=12 and randomSeed=10, but we need to round it
        assertAggregation(
                noisyCountIfGaussian,
                equalDoubleAssertion,
                "Test noisy_count_if_gaussian(boolean, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createBooleansBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                expected);
    }

    // Test BOOLEAN  0-row input returns NULL
    @Test
    public void testNoisyCountIfGaussianNoInputRowsWithoutGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BOOLEAN,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BOOLEAN);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0) + 1 FROM " + data
                + " WHERE false";

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 1);
        assertNull(actualRows.get(0).getField(0));
    }

    @Test
    public void testNoisyCountIfGaussianNoInputRowsWithGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(
                StandardTypes.BOOLEAN,
                StandardTypes.DOUBLE,
                StandardTypes.REAL,
                StandardTypes.DECIMAL));
        String columnName = buildColumnName(StandardTypes.BOOLEAN);
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
