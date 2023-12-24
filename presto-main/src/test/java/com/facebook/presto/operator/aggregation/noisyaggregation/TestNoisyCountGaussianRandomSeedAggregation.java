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
import com.facebook.presto.common.type.NamedType;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static com.facebook.presto.common.type.QuantileDigestParametricType.QDIGEST;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TDigestParametricType.TDIGEST;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.DEFAULT_TEST_STANDARD_DEVIATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildColumnName;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.buildData;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.createTestValues;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.equalLongAssertion;
import static com.facebook.presto.operator.aggregation.noisyaggregation.TestNoisyAggregationUtils.withinSomeStdAssertion;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.facebook.presto.type.RowParametricType.ROW;
import static com.facebook.presto.type.khyperloglog.KHyperLogLogType.K_HYPER_LOG_LOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestNoisyCountGaussianRandomSeedAggregation
        extends AbstractTestFunctions
{
    private static final String FUNCTION_NAME = "noisy_count_gaussian";
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testNoisyCountGaussianDefinitions()
    {
        // Function is available for all standard data types
        getFunction(TINYINT, DOUBLE, BIGINT);
        getFunction(SMALLINT, DOUBLE, BIGINT);
        getFunction(INTEGER, DOUBLE, BIGINT);
        getFunction(BIGINT, DOUBLE, BIGINT);
        getFunction(REAL, DOUBLE, BIGINT);
        getFunction(DOUBLE, DOUBLE, BIGINT);
        getFunction(createDecimalType(38, 0), DOUBLE, BIGINT);
        getFunction(createDecimalType(18, 0), DOUBLE, BIGINT);
        getFunction(VARCHAR, DOUBLE, BIGINT);
        getFunction(createCharType(1), DOUBLE, BIGINT);
        getFunction(VARBINARY, DOUBLE, BIGINT);
        getFunction(JSON, DOUBLE, BIGINT);
        getFunction(DATE, DOUBLE, BIGINT);
        getFunction(TIME, DOUBLE, BIGINT);
        getFunction(TIME_WITH_TIME_ZONE, DOUBLE, BIGINT);
        getFunction(TIMESTAMP, DOUBLE, BIGINT);
        getFunction(TIMESTAMP_WITH_TIME_ZONE, DOUBLE, BIGINT);
        getFunction(INTERVAL_DAY_TIME, DOUBLE, BIGINT);
        getFunction(INTERVAL_YEAR_MONTH, DOUBLE, BIGINT);
        getFunction(ARRAY.createType(ImmutableList.of(TypeParameter.of(DOUBLE))), DOUBLE, BIGINT);
        getFunction(MAP.createType(FunctionAndTypeManager.createTestFunctionAndTypeManager(), ImmutableList.of(TypeParameter.of(BIGINT), TypeParameter.of(DOUBLE))), DOUBLE, BIGINT);
        getFunction(ROW.createType(ImmutableList.of(TypeParameter.of(new NamedType(Optional.of(new RowFieldName("x", false)), DOUBLE)))), DOUBLE, BIGINT);
        getFunction(IPADDRESS, DOUBLE, BIGINT);
        getFunction(IPPREFIX, DOUBLE, BIGINT);
        getFunction(UUID, DOUBLE, BIGINT);
        getFunction(HYPER_LOG_LOG, DOUBLE, BIGINT);
        getFunction(P4_HYPER_LOG_LOG, DOUBLE, BIGINT);
        getFunction(K_HYPER_LOG_LOG, DOUBLE, BIGINT);
        getFunction(QDIGEST.createType(ImmutableList.of(TypeParameter.of(DOUBLE))), DOUBLE, BIGINT);
        getFunction(TDIGEST.createType(ImmutableList.of(TypeParameter.of(DOUBLE))), DOUBLE, BIGINT);
    }

    @Test
    public void testNoisyCountGaussianRandomSeedLongZeroNoiseScaleZeroRandomSeed()
    {
        // Test COUNT(col, 0, 0)
        JavaAggregationFunctionImplementation noisyCountGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        assertAggregation(
                noisyCountGaussian,
                equalLongAssertion,
                "Test noisy_count_gaussian(long, noiseScale, randomSeed) with noiseScale=0 which means no noise",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(0.0, numRows),
                        createRLEBlock(0, numRows)),
                numRows);
    }

    @Test
    public void testNoisyCountGaussianRandomSeedLongSomeNoiseScaleFixedRandomSeed()
    {
        // Test COUNT(col, 12, 10)
        JavaAggregationFunctionImplementation noisyCountGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        assertAggregation(
                noisyCountGaussian,
                equalLongAssertion,
                "Test noisy_count_gaussian(long, noiseScale, randomSeed) with noiseScale=12 which there is some noise, and a fixed random seed",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(12.0, numRows),
                        createRLEBlock(10, numRows)),
                1010); // 1010 is when numRows=1000, noiseScale=12 and randomSeed=10
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Noise scale must be >= 0")
    public void testNoisyCountGaussianRandomSeedLongInvalidNoiseScale()
    {
        // Test COUNT(col, -123, 10)
        JavaAggregationFunctionImplementation noisyCountGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        assertAggregation(
                noisyCountGaussian,
                equalLongAssertion,
                "Test noisy_count_gaussian(long, noiseScale, randomSeed) with noiseScale < 0 which we expect an error",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(-123.0, numRows),
                        createRLEBlock(10, numRows)),
                numRows);
    }

    @Test
    public void testNoisyCountGaussianRandomSeedLongRandomNoiseWithinSomeStd()
    {
        // Test COUNT(col, 100)
        JavaAggregationFunctionImplementation noisyCountGaussian = getFunction(BIGINT, DOUBLE, BIGINT);

        int numRows = 1000;
        List<Long> values = createTestValues(numRows, false, 1L, true);
        assertAggregation(
                noisyCountGaussian,
                withinSomeStdAssertion,
                "Test noisy_count_gaussian(long, noiseScale) with noiseScale=DEFAULT_TEST_STANDARD_DEVIATION and expect result is within some std from mean",
                new Page(
                        createLongsBlock(values),
                        createRLEBlock(DEFAULT_TEST_STANDARD_DEVIATION, numRows),
                        createRLEBlock(10, numRows)),
                numRows); // expected mean
    }

    @Test
    public void testNoisyCountGaussianRandomSeedNoInputRowsWithoutGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(StandardTypes.BIGINT, StandardTypes.VARCHAR));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0, 1) + 1 FROM " + data
                + " WHERE false";

        List<MaterializedRow> actualRows = runQuery(query);
        assertEquals(actualRows.size(), 1);
        assertNull(actualRows.get(0).getField(0));
    }

    @Test
    public void testNoisyCountGaussianRandomSeedNoInputRowsWithGroupBy()
    {
        int numRows = 100;
        String data = buildData(numRows, true, Arrays.asList(StandardTypes.BIGINT, StandardTypes.VARCHAR));
        String columnName = buildColumnName(StandardTypes.BIGINT);
        String query = "SELECT " + FUNCTION_NAME + "(" + columnName + ", 0, 1) + 1 FROM " + data
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
