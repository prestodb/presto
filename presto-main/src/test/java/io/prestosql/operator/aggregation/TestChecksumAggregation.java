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
package io.prestosql.operator.aggregation;

import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.block.BlockAssertions.createArrayBigintBlock;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongDecimalsBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createShortDecimalsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.operator.aggregation.ChecksumAggregationFunction.PRIME64;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Arrays.asList;

public class TestChecksumAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @Test
    public void testEmpty()
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("checksum",
                        AGGREGATE,
                        parseTypeSignature(VARBINARY),
                        parseTypeSignature(BOOLEAN)));
        assertAggregation(booleanAgg, null, createBooleansBlock());
    }

    @Test
    public void testBoolean()
    {
        InternalAggregationFunction booleanAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("checksum",
                        AGGREGATE,
                        parseTypeSignature(VARBINARY),
                        parseTypeSignature(BOOLEAN)));
        Block block = createBooleansBlock(null, null, true, false, false);
        assertAggregation(booleanAgg, expectedChecksum(BooleanType.BOOLEAN, block), block);
    }

    @Test
    public void testLong()
    {
        InternalAggregationFunction longAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("checksum",
                        AGGREGATE,
                        parseTypeSignature(VARBINARY),
                        parseTypeSignature(BIGINT)));
        Block block = createLongsBlock(null, 1L, 2L, 100L, null, Long.MAX_VALUE, Long.MIN_VALUE);
        assertAggregation(longAgg, expectedChecksum(BigintType.BIGINT, block), block);
    }

    @Test
    public void testDouble()
    {
        InternalAggregationFunction doubleAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("checksum",
                        AGGREGATE,
                        parseTypeSignature(VARBINARY),
                        parseTypeSignature(DOUBLE)));
        Block block = createDoublesBlock(null, 2.0, null, 3.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN);
        assertAggregation(doubleAgg, expectedChecksum(DoubleType.DOUBLE, block), block);
    }

    @Test
    public void testString()
    {
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("checksum",
                        AGGREGATE,
                        parseTypeSignature(VARBINARY),
                        parseTypeSignature(VARCHAR)));
        Block block = createStringsBlock("a", "a", null, "b", "c");
        assertAggregation(stringAgg, expectedChecksum(VarcharType.VARCHAR, block), block);
    }

    @Test
    public void testShortDecimal()
    {
        InternalAggregationFunction decimalAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("checksum", AGGREGATE, parseTypeSignature(VARBINARY), parseTypeSignature("decimal(10,2)")));
        Block block = createShortDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType shortDecimalType = DecimalType.createDecimalType(1);
        assertAggregation(decimalAgg, expectedChecksum(shortDecimalType, block), block);
    }

    @Test
    public void testLongDecimal()
    {
        InternalAggregationFunction decimalAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("checksum", AGGREGATE, parseTypeSignature(VARBINARY), parseTypeSignature("decimal(19,2)")));
        Block block = createLongDecimalsBlock("11.11", "22.22", null, "33.33", "44.44");
        DecimalType longDecimalType = DecimalType.createDecimalType(19);
        assertAggregation(decimalAgg, expectedChecksum(longDecimalType, block), block);
    }

    @Test
    public void testArray()
    {
        ArrayType arrayType = new ArrayType(BigintType.BIGINT);
        InternalAggregationFunction stringAgg = metadata.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("checksum", AGGREGATE, VarbinaryType.VARBINARY.getTypeSignature(), arrayType.getTypeSignature()));
        Block block = createArrayBigintBlock(asList(null, asList(1L, 2L), asList(3L, 4L), asList(5L, 6L)));
        assertAggregation(stringAgg, expectedChecksum(arrayType, block), block);
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
}
