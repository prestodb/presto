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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public class TestBlackholeAggregation
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
    private static final SqlVarbinary EXPECTED_OUTPUT = new SqlVarbinary("OK".getBytes(UTF_8));

    @Test
    public void testEmpty()
    {
        JavaAggregationFunctionImplementation booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(booleanAgg, EXPECTED_OUTPUT, createBooleansBlock());
    }

    @Test
    public void testBoolean()
    {
        JavaAggregationFunctionImplementation booleanAgg = getAggregation(BOOLEAN);
        Block block = createBooleansBlock(null, null, true, false, false);
        assertAggregation(booleanAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testLong()
    {
        JavaAggregationFunctionImplementation longAgg = getAggregation(BIGINT);
        Block block = createLongsBlock(null, 1L, 2L, 100L, null, Long.MAX_VALUE, Long.MIN_VALUE);
        assertAggregation(longAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testDouble()
    {
        JavaAggregationFunctionImplementation doubleAgg = getAggregation(DOUBLE);
        Block block = createDoublesBlock(null, 2.0, null, 3.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        assertAggregation(doubleAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testString()
    {
        JavaAggregationFunctionImplementation stringAgg = getAggregation(VARCHAR);
        Block block = createStringsBlock("a", "a", null, "b", "c");
        assertAggregation(stringAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testShortDecimal()
    {
        JavaAggregationFunctionImplementation decimalAgg = getAggregation(createDecimalType(10, 2));
        Block block = createShortDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        assertAggregation(decimalAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testLongDecimal()
    {
        JavaAggregationFunctionImplementation decimalAgg = getAggregation(createDecimalType(19, 2));
        Block block = createLongDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        assertAggregation(decimalAgg, EXPECTED_OUTPUT, block);
    }

    @Test
    public void testArray()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        JavaAggregationFunctionImplementation stringAgg = getAggregation(arrayType);
        Block block = createArrayBigintBlock(asList(null, asList(1L, 2L), asList(3L, 4L), asList(5L, 6L)));
        assertAggregation(stringAgg, EXPECTED_OUTPUT, block);
    }

    private JavaAggregationFunctionImplementation getAggregation(Type argument)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("blackhole", fromTypes(argument)));
    }
}
