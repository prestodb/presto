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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.RunLengthEncodedBlock;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.type.UnknownType.UNKNOWN;

public abstract class AbstractTestAggregationFunction
{
    protected final TypeRegistry typeRegistry = new TypeRegistry();
    protected final FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), true);
    public abstract Block getSequenceBlock(int start, int length);

    protected final InternalAggregationFunction getFunction()
    {
        return functionRegistry.resolveFunction(QualifiedName.of(getFunctionName()), Lists.transform(getFunctionParameterTypes(), TypeSignature::parseTypeSignature), isApproximate()).getAggregationFunction();
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
        testAggregation(getExpectedValue(0, 0), getSequenceBlock(0, 0));
    }

    @Test
    public void testSinglePosition()
    {
        testAggregation(getExpectedValue(0, 1), getSequenceBlock(0, 1));
    }

    @Test
    public void testMultiplePositions()
    {
        testAggregation(getExpectedValue(0, 5), getSequenceBlock(0, 5));
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

        Block nullValueBlock = parameterTypes.get(0).createBlockBuilder(new BlockBuilderStatus(), 1, UNKNOWN.getFixedSize())
                .appendNull()
                .build();

        Block block = new RunLengthEncodedBlock(nullValueBlock, 10);
        testAggregation(getExpectedValueIncludingNulls(0, 0, 10), block);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }

        Block alternatingNullsBlock = createAlternatingNullsBlock(parameterTypes.get(0), getSequenceBlock(0, 10));
        testAggregation(getExpectedValueIncludingNulls(0, 10, 20), alternatingNullsBlock);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlock(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlock(2, 4));
    }

    public Block createAlternatingNullsBlock(Type type, Block sequenceBlock)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), sequenceBlock.getPositionCount() * 2);
        for (int position = 0; position < sequenceBlock.getPositionCount(); position++) {
            // append null
            blockBuilder.appendNull();
            // append value
            type.appendTo(sequenceBlock, position, blockBuilder);
        }
        return blockBuilder.build();
    }

    protected void testAggregation(Object expectedValue, Block block)
    {
        assertAggregation(getFunction(), getConfidence(), expectedValue, block.getPositionCount(), block);
    }
}
