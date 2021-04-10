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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTestAggregationFunction
{
    protected FunctionAndTypeManager functionAndTypeManager;
    protected Session session;

    protected AbstractTestAggregationFunction()
    {
        this(testSessionBuilder().build());
    }

    protected AbstractTestAggregationFunction(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    @BeforeClass
    public final void initTestAggregationFunction()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
    }

    @AfterClass(alwaysRun = true)
    public final void destroyTestAggregationFunction()
    {
        functionAndTypeManager = null;
    }

    public abstract Block[] getSequenceBlocks(int start, int length);

    protected void registerFunctions(Plugin plugin)
    {
        functionAndTypeManager.registerBuiltInFunctions(extractFunctions(plugin.getFunctions()));
    }

    protected void registerTypes(Plugin plugin)
    {
        for (Type type : plugin.getTypes()) {
            functionAndTypeManager.addType(type);
        }
    }

    protected final InternalAggregationFunction getFunction()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypeSignatures(Lists.transform(getFunctionParameterTypes(), TypeSignature::parseTypeSignature));
        FunctionHandle functionHandle = functionAndTypeManager.resolveFunction(
                Optional.empty(),
                session.getTransactionId(),
                qualifyObjectName(QualifiedName.of(getFunctionName())),
                parameterTypes);
        return functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
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
