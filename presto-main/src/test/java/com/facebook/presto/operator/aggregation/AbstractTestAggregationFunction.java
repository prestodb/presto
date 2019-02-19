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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.window.PagesWindowIndex;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.block.BlockAssertions.getOnlyValue;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static com.facebook.presto.operator.aggregation.groupByAggregations.GroupByAggregationTestUtils.createArgs;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestAggregationFunction
{
    protected TypeRegistry typeRegistry;
    protected FunctionManager functionManager;
    protected Session session;

    @BeforeClass
    public final void initTestAggregationFunction()
    {
        typeRegistry = new TypeRegistry();
        functionManager = new FunctionManager(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        session = testSessionBuilder().build();
    }

    @AfterClass(alwaysRun = true)
    public final void destroyTestAggregationFunction()
    {
        functionManager = null;
        typeRegistry = null;
    }

    public abstract Block[] getSequenceBlocks(int start, int length);

    protected void registerFunctions(Plugin plugin)
    {
        functionManager.addFunctions(extractFunctions(plugin.getFunctions()));
    }

    protected void registerTypes(Plugin plugin)
    {
        for (Type type : plugin.getTypes()) {
            typeRegistry.addType(type);
        }
    }

    protected final InternalAggregationFunction getFunction()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypeSignatures(Lists.transform(getFunctionParameterTypes(), TypeSignature::parseTypeSignature));
        FunctionHandle functionHandle = functionManager.resolveFunction(session, QualifiedName.of(getFunctionName()), parameterTypes);
        return functionManager.getAggregateFunctionImplementation(functionHandle);
    }

    protected abstract String getFunctionName();

    protected abstract List<String> getFunctionParameterTypes();

    public abstract Object getExpectedValue(int start, int length);

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
    {
        // if there are no parameters skip this test
        List<Type> parameterTypes = getFunction().getParameterTypes();
        if (parameterTypes.isEmpty()) {
            return;
        }
        Block[] blocks = new Block[parameterTypes.size()];
        for (int i = 0; i < parameterTypes.size(); i++) {
            blocks[i] = RunLengthEncodedBlock.create(parameterTypes.get(0), null, 10);
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

        Block[] alternatingNullsBlocks = createAlternatingNullsBlock(parameterTypes, getSequenceBlocks(0, 10));
        testAggregation(getExpectedValueIncludingNulls(0, 10, 20), alternatingNullsBlocks);
    }

    @Test
    public void testNegativeOnlyValues()
    {
        testAggregation(getExpectedValue(-10, 5), getSequenceBlocks(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
    {
        testAggregation(getExpectedValue(2, 4), getSequenceBlocks(2, 4));
    }

    @Test
    public void testSlidingWindow()
    {
        // Builds trailing windows of length 0, 1, 2, 3, 4, 5, 5, 4, 3, 2, 1, 0
        int totalPositions = 12;
        int[] windowWidths = new int[totalPositions];
        Object[] expectedValues = new Object[totalPositions];

        for (int i = 0; i < totalPositions; ++i) {
            int windowWidth = Integer.min(i, totalPositions - 1 - i);
            windowWidths[i] = windowWidth;
            expectedValues[i] = getExpectedValue(i, windowWidth);
        }
        Page inputPage = new Page(totalPositions, getSequenceBlocks(0, totalPositions));

        InternalAggregationFunction function = getFunction();
        List<Integer> channels = Ints.asList(createArgs(function));
        AccumulatorFactory accumulatorFactory = function.bind(channels, Optional.empty());
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(function.getParameterTypes(), totalPositions);
        pagesIndex.addPage(inputPage);
        WindowIndex windowIndex = new PagesWindowIndex(pagesIndex, 0, totalPositions - 1);

        Accumulator aggregation = accumulatorFactory.createAccumulator();
        int oldStart = 0;
        int oldWidth = 0;
        for (int start = 0; start < totalPositions; ++start) {
            int width = windowWidths[start];
            // Note that add/removeInput's interval is inclusive on both ends
            if (accumulatorFactory.hasRemoveInput()) {
                for (int oldi = oldStart; oldi < oldStart + oldWidth; ++oldi) {
                    if (oldi < start || oldi >= start + width) {
                        aggregation.removeInput(windowIndex, channels, oldi, oldi);
                    }
                }
                for (int newi = start; newi < start + width; ++newi) {
                    if (newi < oldStart || newi >= oldStart + oldWidth) {
                        aggregation.addInput(windowIndex, channels, newi, newi);
                    }
                }
            }
            else {
                aggregation = accumulatorFactory.createAccumulator();
                aggregation.addInput(windowIndex, channels, start, start + width - 1);
            }
            oldStart = start;
            oldWidth = width;
            Block block = getFinalBlock(aggregation);
            // TODO: let's see if we can do approx equial for double, float, etc
            // ref: https://github.com/prestodb/presto/pull/8974/commits/9c7b7f7d49af653c30ebe400bac32241af81a89b
            assertEquals(expectedValues[start], getOnlyValue(aggregation.getFinalType(), block));
        }
    }

    public Block[] createAlternatingNullsBlock(List<Type> types, Block... sequenceBlocks)
    {
        Block[] alternatingNullsBlocks = new Block[sequenceBlocks.length];
        for (int i = 0; i < sequenceBlocks.length; i++) {
            int positionCount = sequenceBlocks[i].getPositionCount();
            Type type = types.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                // append null
                blockBuilder.appendNull();
                // append value
                type.appendTo(sequenceBlocks[i], position, blockBuilder);
            }
            alternatingNullsBlocks[i] = blockBuilder.build();
        }
        return alternatingNullsBlocks;
    }

    protected void testAggregation(Object expectedValue, Block... blocks)
    {
        assertAggregation(getFunction(), expectedValue, blocks);
    }
}
