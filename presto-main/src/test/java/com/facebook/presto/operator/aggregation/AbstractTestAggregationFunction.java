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

import com.facebook.presto.testing.RunLengthEncodedBlock;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public abstract class AbstractTestAggregationFunction
{
    protected final FunctionRegistry functionRegistry = new FunctionRegistry(new TypeRegistry(), true);
    public abstract Block[] getSequenceBlocks(int start, int length);

    protected final InternalAggregationFunction getFunction()
    {
        return functionRegistry.resolveFunction(QualifiedName.of(getFunctionName()), Lists.transform(getFunctionParameterTypes(), TypeUtils.typeSignatureParser()), isApproximate()).getAggregationFunction();
    }

    protected abstract String getFunctionName();

    protected abstract List<String> getFunctionParameterTypes();

    protected boolean isApproximate()
    {
        return false;
    }

    public abstract Object getExpectedValue(int start, int length);

    public double getConfidence()
    {
        return 1.0;
    }

    public Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return getExpectedValue(start, length);
    }

    @Test
    public void testNoPositions()
    {
        testAggregation(getExpectedValue(0, 0), getSequenceBlocks(0, 0));
    }

    @Test
    public void testSinglePosition()
    {
        testAggregation(getExpectedValue(0, 1), getSequenceBlocks(0, 1));
    }

    @Test
    public void testMultiplePositions()
    {
        testAggregation(getExpectedValue(0, 5), getSequenceBlocks(0, 5));
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }
        Block[] blocks = new Block[parameterTypes.size()];
        for (int i = 0; i < parameterTypes.size(); i++) {
            Block nullValueBlock = parameterTypes.get(0).createBlockBuilder(new BlockBuilderStatus())
                    .appendNull()
                    .build();
            blocks[i] = new RunLengthEncodedBlock(nullValueBlock, 10);
        }

        testAggregation(getExpectedValueIncludingNulls(0, 0, 10), blocks);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }

        Block[] alternatingNullsBlocks = createAlternatingNullsBlock(parameterTypes.get(0), getSequenceBlocks(0, 5));
        testAggregation(getExpectedValueIncludingNulls(0, 5, 10), alternatingNullsBlocks);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlocks(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 5), getSequenceBlocks(2, 5));
    }

    public Block[] createAlternatingNullsBlock(Type type, Block... sequenceBlocks)
    {
        Block[] alternatingNullsBlocks = new Block[sequenceBlocks.length];
        for (int i = 0; i < sequenceBlocks.length; i++) {
            BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
            for (int position = 0; position < sequenceBlocks[i].getPositionCount(); position++) {
                // append null
                blockBuilder.appendNull();
                // append value
                type.appendTo(sequenceBlocks[i], position, blockBuilder);
                alternatingNullsBlocks[i] = blockBuilder.build();
            }
        }
        return alternatingNullsBlocks;
    }

    protected void testAggregation(Object expectedValue, Block... blocks)
    {
        assertAggregation(getFunction(), getConfidence(), expectedValue, blocks[0].getPositionCount(), blocks);
    }

    protected double[] constructDoublePrimitiveArray(int start, int length)
    {
        double[] values = new double[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }
        return values;
    }

    protected Block getDoubleSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, i);
        }
        return blockBuilder.build();
    }
}
