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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.block.MapBlock.fromKeyValueBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.ChecksumAggregationFunction.PRIME64;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.util.Arrays.asList;

public class TestChecksumAggregation
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testEmpty()
    {
        JavaAggregationFunctionImplementation booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(booleanAgg, null, createBooleansBlock());
    }

    @Test
    public void testBoolean()
    {
        JavaAggregationFunctionImplementation booleanAgg = getAggregation(BOOLEAN);
        Block block = createBooleansBlock(null, null, true, false, false);
        assertAggregation(booleanAgg, expectedChecksum(BOOLEAN, block), block);
    }

    @Test
    public void testLong()
    {
        JavaAggregationFunctionImplementation longAgg = getAggregation(BIGINT);
        Block block = createLongsBlock(null, 1L, 2L, 100L, null, Long.MAX_VALUE, Long.MIN_VALUE);
        assertAggregation(longAgg, expectedChecksum(BIGINT, block), block);
    }

    @Test
    public void testDouble()
    {
        JavaAggregationFunctionImplementation doubleAgg = getAggregation(DOUBLE);
        Block block = createDoublesBlock(null, 2.0, null, 3.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        assertAggregation(doubleAgg, expectedChecksum(DOUBLE, block), block);
    }

    @Test
    public void testString()
    {
        JavaAggregationFunctionImplementation stringAgg = getAggregation(VARCHAR);
        Block block = createStringsBlock("a", "a", null, "b", "c");
        assertAggregation(stringAgg, expectedChecksum(VARCHAR, block), block);
    }

    @Test
    public void testShortDecimal()
    {
        JavaAggregationFunctionImplementation decimalAgg = getAggregation(createDecimalType(10, 2));
        Block block = createShortDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType shortDecimalType = createDecimalType(1);
        assertAggregation(decimalAgg, expectedChecksum(shortDecimalType, block), block);
    }

    @Test
    public void testLongDecimal()
    {
        JavaAggregationFunctionImplementation decimalAgg = getAggregation(createDecimalType(19, 2));
        Block block = createLongDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType longDecimalType = createDecimalType(19);
        assertAggregation(decimalAgg, expectedChecksum(longDecimalType, block), block);
    }

    @Test
    public void testArray()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        JavaAggregationFunctionImplementation stringAgg = getAggregation(arrayType);
        Block block = createArrayBigintBlock(asList(null, asList(1L, 2L), asList(3L, 4L), asList(5L, 6L)));
        assertAggregation(stringAgg, expectedChecksum(arrayType, block), block);
    }

    @Test
    public void testMap()
    {
        MapType mapType = createMapType(BIGINT, DOUBLE);
        JavaAggregationFunctionImplementation mapAgg = getAggregation(mapType);
        Map<Long, Double> map = ImmutableMap.of(1L, 23.45, 2L, 345.678, 3L, 4567.8901);
        Block block = createMapBlock(mapType, map);
        assertAggregation(mapAgg, expectedChecksum(mapType, block), block);

        // Multiple maps Map<BIGINT, DOUBLE>
        long[] keys = {25895L, 111936L, 162115L, 304417L, 3069541L, 3159396L, 3500641L, 5097671L, 5197414L, 5404804L};
        LongArrayBlock keyBlock = new LongArrayBlock(10, Optional.empty(), keys);
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, 10);
        double[] values = {149912.63, 264526.94, 90483.15, 150554.2, 195291.64, 328452.73, 10938.72, 12740.73, 31276.6, 119651.44};
        for (int i = 0; i < values.length; i++) {
            DOUBLE.writeDouble(builder, values[i]);
        }
        Block valueBlock = builder.build();
        int[] offsets = {0, 2, 6, 10};
        Block multiMapBlock = fromKeyValueBlock(3, Optional.ofNullable(null), offsets, keyBlock, valueBlock);
        assertAggregation(mapAgg, expectedChecksum(mapType, multiMapBlock), multiMapBlock);
    }

    private static SqlVarbinary expectedChecksum(Type type, Block block)
    {
        long result = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                result += PRIME64;
            }
            else {
                result += type.hash(block, i) * PRIME64;
            }
        }
        return new SqlVarbinary(wrappedLongArray(result).getBytes());
    }

    private JavaAggregationFunctionImplementation getAggregation(Type argument)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("checksum", fromTypes(argument)));
    }
}
