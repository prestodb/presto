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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByState;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByStateFactory;
import com.facebook.presto.operator.aggregation.state.MaxOrMinByStateSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createArrayBigintBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestMinMaxByAggregation
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @BeforeClass
    public void setup()
    {
        ((TypeRegistry) METADATA.getTypeManager()).addType(CustomDoubleType.CUSTOM_DOUBLE);
    }

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = METADATA.getTypeManager()
                .getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : METADATA.getTypeManager().getTypes()) {
                assertNotNull(METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("min_by", AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature())));
                assertNotNull(METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("max_by", AGGREGATE, valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature())));
            }
        }
    }

    @Test
    public void testMinNull()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("min_by", AGGREGATE, StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                1.0,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
    }

    @Test
    public void testMaxNull()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("max_by", AGGREGATE, StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                null,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
    }

    @Test
    public void testMinDoubleDouble()
            throws Exception
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("min_by", AGGREGATE, StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                1.0,
                3.0,
                createDoublesBlock(3.0, 2.0, 5.0, 3.0),
                createDoublesBlock(1.0, 1.5, 2.0, 4.0));
    }

    @Test
    public void testMaxDoubleDouble()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("max_by", AGGREGATE, StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                function,
                1.0,
                2.0,
                createDoublesBlock(3.0, 2.0, null),
                createDoublesBlock(1.0, 1.5, null));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("min_by", AGGREGATE, StandardTypes.VARCHAR, StandardTypes.VARCHAR, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0));

        assertAggregation(
                function,
                1.0,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("max_by", AGGREGATE, StandardTypes.VARCHAR, StandardTypes.VARCHAR, StandardTypes.DOUBLE));
        assertAggregation(
                function,
                1.0,
                "a",
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                function,
                1.0,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0));
    }

    @Test
    public void testMinLongLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("min_by", AGGREGATE, "array(bigint)", "array(bigint)", StandardTypes.BIGINT));
        assertAggregation(
                function,
                1.0,
                ImmutableList.of(8L, 9L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L))),
                createLongsBlock(1L, 2L, 2L, 3L));

        assertAggregation(
                function,
                1.0,
                ImmutableList.of(2L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L), ImmutableList.of(2L))),
                createLongsBlock(0L, 1L, 2L, -1L));
    }

    @Test
    public void testMaxLongLongArray()
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(new Signature("max_by", AGGREGATE, "array(bigint)", "array(bigint)", StandardTypes.BIGINT));
        assertAggregation(
                function,
                1.0,
                ImmutableList.of(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(1L, 2L), null)),
                createLongsBlock(1L, 2L, null));

        assertAggregation(
                function,
                1.0,
                ImmutableList.of(2L, 3L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(2L, 3L), null, asList(1L, 2L))),
                createLongsBlock(0L, 1L, null, -1L));
    }

    @Test
    public void testStateDeserializer()
            throws Exception
    {
        String[] keys = {"loooooong string", "short string"};
        double[] values = {3.14, 2.71};

        MaxOrMinByStateSerializer serializer = new MaxOrMinByStateSerializer(DOUBLE, VARCHAR);
        BlockBuilder builder = new RowType(ImmutableList.of(VARCHAR, DOUBLE), Optional.empty()).createBlockBuilder(new BlockBuilderStatus(), 2);

        for (int i = 0; i < keys.length; i++) {
            serializer.serialize(makeState(keys[i], values[i]), builder);
        }

        Block serialized = builder.build();

        for (int i = 0; i < keys.length; i++) {
            MaxOrMinByState deserialized = new MaxOrMinByStateFactory().createSingleState();
            serializer.deserialize(serialized, i, deserialized);
            assertEquals(VARCHAR.getSlice(deserialized.getKey(), 0), Slices.utf8Slice(keys[i]));
            assertEquals(DOUBLE.getDouble(deserialized.getValue(), 0), values[i]);
        }
    }

    private static MaxOrMinByState makeState(String key, double value)
    {
        MaxOrMinByState result = new MaxOrMinByStateFactory().createSingleState();
        result.setKey(createStringsBlock(key));
        result.setValue(createDoublesBlock(value));
        return result;
    }

    private static class CustomDoubleType
            extends AbstractFixedWidthType
    {
        public static final CustomDoubleType CUSTOM_DOUBLE = new CustomDoubleType();
        public static final String NAME = "custom_double";

        private CustomDoubleType()
        {
            super(parseTypeSignature(NAME), double.class, SIZE_OF_DOUBLE);
        }

        @Override
        public boolean isComparable()
        {
            return true;
        }

        @Override
        public boolean isOrderable()
        {
            return true;
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            if (block.isNull(position)) {
                return null;
            }
            return block.getDouble(position, 0);
        }

        @Override
        public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            long leftValue = leftBlock.getLong(leftPosition, 0);
            long rightValue = rightBlock.getLong(rightPosition, 0);
            return leftValue == rightValue;
        }

        @Override
        public long hash(Block block, int position)
        {
            long value = block.getLong(position, 0);
            return (int) (value ^ (value >>> 32));
        }

        @Override
        public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            double leftValue = leftBlock.getDouble(leftPosition, 0);
            double rightValue = rightBlock.getDouble(rightPosition, 0);
            return Double.compare(leftValue, rightValue);
        }

        @Override
        public void appendTo(Block block, int position, BlockBuilder blockBuilder)
        {
            if (block.isNull(position)) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeDouble(block.getDouble(position, 0)).closeEntry();
            }
        }

        @Override
        public double getDouble(Block block, int position)
        {
            return block.getDouble(position, 0);
        }

        @Override
        public void writeDouble(BlockBuilder blockBuilder, double value)
        {
            blockBuilder.writeDouble(value).closeEntry();
        }
    }
}
