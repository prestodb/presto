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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapVarcharBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createRowVarcharBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.ChecksumAggregationFunction.PRIME64;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertNotEquals;

public class TestChecksumAggregation
{
    private static final FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();

    @Test
    public void testEmpty()
    {
        InternalAggregationFunction booleanAgg = getAggregation(BOOLEAN);
        assertAggregation(booleanAgg, null, createBooleansBlock());
    }

    @Test
    public void testBoolean()
    {
        InternalAggregationFunction booleanAgg = getAggregation(BOOLEAN);
        Block block = createBooleansBlock(null, null, true, false, false);
        assertAggregation(booleanAgg, expectedChecksum(BOOLEAN, block), block);
    }

    @Test
    public void testLong()
    {
        InternalAggregationFunction longAgg = getAggregation(BIGINT);
        Block block = createLongsBlock(null, 1L, 2L, 100L, null, Long.MAX_VALUE, Long.MIN_VALUE);
        assertAggregation(longAgg, expectedChecksum(BIGINT, block), block);
    }

    @Test
    public void testDouble()
    {
        InternalAggregationFunction doubleAgg = getAggregation(DOUBLE);
        Block block = createDoublesBlock(null, 2.0, null, 3.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        assertAggregation(doubleAgg, expectedChecksum(DOUBLE, block), block);
    }

    @Test
    public void testString()
    {
        InternalAggregationFunction stringAgg = getAggregation(VARCHAR);
        Block block = createStringsBlock("a", "a", null, "b", "c");
        assertAggregation(stringAgg, expectedChecksum(VARCHAR, block), block);
    }

    @Test
    public void testShortDecimal()
    {
        InternalAggregationFunction decimalAgg = getAggregation(createDecimalType(10, 2));
        Block block = createShortDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType shortDecimalType = createDecimalType(1);
        assertAggregation(decimalAgg, expectedChecksum(shortDecimalType, block), block);
    }

    @Test
    public void testLongDecimal()
    {
        InternalAggregationFunction decimalAgg = getAggregation(createDecimalType(19, 2));
        Block block = createLongDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType longDecimalType = createDecimalType(19);
        assertAggregation(decimalAgg, expectedChecksum(longDecimalType, block), block);
    }

    @Test
    public void testArray()
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        InternalAggregationFunction stringAgg = getAggregation(arrayType);
        Block block = createArrayBigintBlock(asList(null, asList(1L, 2L), asList(3L, 4L), asList(5L, 6L)));
        assertAggregation(stringAgg, expectedChecksum(arrayType, block), block);
    }

    @Test
    public void testStructuralChecksumCollision()
    {
        ArrayType arrayType = new ArrayType(BigintType.BIGINT);
        for (int i = 0; i < 100; i++) {
            long a1 = ThreadLocalRandom.current().nextLong();
            long b1 = ThreadLocalRandom.current().nextLong();
            long a2 = ThreadLocalRandom.current().nextLong();
            long b2 = ThreadLocalRandom.current().nextLong();

            Block arrayBlock1 = createArrayBigintBlock(ImmutableList.of(
                    ImmutableList.of(a1, b1),
                    ImmutableList.of(a2, b2)));
            Block arrayBlock2 = createArrayBigintBlock(ImmutableList.of(
                    ImmutableList.of(a1, b2),
                    ImmutableList.of(a2, b1)));
            assertNotEquals(expectedChecksum(arrayType, arrayBlock1), expectedChecksum(arrayType, arrayBlock2), "unexpected checksum collision");
        }

        MapType mapType = mapType(VarcharType.VARCHAR, BigintType.BIGINT);
        for (int i = 0; i < 100; i++) {
            String k1 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long v1 = ThreadLocalRandom.current().nextLong();
            String k2 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long v2 = ThreadLocalRandom.current().nextLong();
            String k3 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long v3 = ThreadLocalRandom.current().nextLong();
            String k4 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long v4 = ThreadLocalRandom.current().nextLong();

            Block mapBlock1 = createMapVarcharBigintBlock(ImmutableList.of(
                    ImmutableMap.of(k1, v1, k2, v2),
                    ImmutableMap.of(k3, v3, k4, v4)));
            Block mapBlock2 = createMapVarcharBigintBlock(ImmutableList.of(
                    ImmutableMap.of(k1, v1, k4, v4),
                    ImmutableMap.of(k3, v3, k2, v2)));
            assertNotEquals(expectedChecksum(mapType, mapBlock1), expectedChecksum(mapType, mapBlock2), "unexpected checksum collision");
        }

        RowType rowType = RowType.anonymous(ImmutableList.of(VarcharType.VARCHAR, BigintType.BIGINT));
        for (int i = 0; i < 100; i++) {
            String a1 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long b1 = ThreadLocalRandom.current().nextLong();
            String a2 = Long.valueOf(ThreadLocalRandom.current().nextLong()).toString();
            long b2 = ThreadLocalRandom.current().nextLong();

            Block rowBlock1 = createRowVarcharBigintBlock(ImmutableList.of(
                    ImmutableList.of(a1, b1),
                    ImmutableList.of(a2, b2)));
            Block rowBlock2 = createRowVarcharBigintBlock(ImmutableList.of(
                    ImmutableList.of(a1, b2),
                    ImmutableList.of(a2, b1)));
            assertNotEquals(expectedChecksum(rowType, rowBlock1), expectedChecksum(rowType, rowBlock2), "unexpected checksum collision");
        }
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

    private InternalAggregationFunction getAggregation(Type argument)
    {
        return functionManager.getAggregateFunctionImplementation(functionManager.lookupFunction("checksum", fromTypes(argument)));
    }
}
