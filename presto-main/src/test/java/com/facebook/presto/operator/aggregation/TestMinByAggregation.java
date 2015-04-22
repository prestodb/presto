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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static org.testng.Assert.assertNotNull;

public class TestMinByAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    @BeforeClass
    public void setup()
    {
        ((TypeRegistry) metadata.getTypeManager()).addType(CustomDoubleType.CUSTOM_DOUBLE);
    }

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = metadata.getTypeManager()
                .getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : metadata.getTypeManager().getTypes()) {
                assertNotNull(metadata.getExactFunction(new Signature("min_by", valueType.getTypeSignature(), valueType.getTypeSignature(), keyType.getTypeSignature())));
            }
        }
    }

    @Test
    public void testNull()
            throws Exception
    {
        InternalAggregationFunction doubleDouble = metadata.getExactFunction(new Signature("min_by", StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE)).getAggregationFunction();
        assertAggregation(
                doubleDouble,
                1.0,
                1.0,
                createPage(
                        new Double[] {1.0, null},
                        new Double[] {1.0, 2.0}));
    }

    @Test
    public void testDoubleDouble()
            throws Exception
    {
        InternalAggregationFunction doubleDouble = metadata.getExactFunction(new Signature("min_by", StandardTypes.DOUBLE, StandardTypes.DOUBLE, StandardTypes.DOUBLE)).getAggregationFunction();
        assertAggregation(
                doubleDouble,
                1.0,
                null,
                createPage(
                        new Double[] {null},
                        new Double[] {null}),
                createPage(
                        new Double[] {null},
                        new Double[] {null}));

        assertAggregation(
                doubleDouble,
                1.0,
                3.0,
                createPage(
                        new Double[] {3.0, 2.0},
                        new Double[] {1.0, 1.5}),
                createPage(
                        new Double[] {5.0, 3.0},
                        new Double[] {2.0, 4.0}));
    }

    @Test
    public void testDoubleVarchar()
            throws Exception
    {
        InternalAggregationFunction doubleVarchar = metadata.getExactFunction(new Signature("min_by", StandardTypes.VARCHAR, StandardTypes.VARCHAR, StandardTypes.DOUBLE)).getAggregationFunction();
        assertAggregation(
                doubleVarchar,
                1.0,
                "z",
                createPage(
                        new String[] {"z", "a"},
                        new Double[] {1.0, 2.0}),
                createPage(
                        new String[] {"x", "b"},
                        new Double[] {2.0, 3.0}));

        assertAggregation(
                doubleVarchar,
                1.0,
                "a",
                createPage(
                        new String[] {"zz", "hi"},
                        new Double[] {0.0, 1.0}),
                createPage(
                        new String[] {"bb", "a"},
                        new Double[] {2.0, -1.0}));
    }

    private static Page createPage(Double[] values, Double[] keys)
    {
        return new Page(createDoublesBlock(values), createDoublesBlock(keys));
    }

    private static Page createPage(String[] values, Double[] keys)
    {
        return new Page(createStringsBlock(values), createDoublesBlock(keys));
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
        public int hash(Block block, int position)
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
