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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getIntermediateBlock;
import static com.facebook.presto.operator.aggregation.GenericAccumulatorFactory.generateAccumulatorFactory;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestAccumulatorAddInputInterface
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public TestAccumulatorAddInputInterface()
    {
        functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
    }

    @Test
    public void testCount()
    {
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT));

        for (int i = 0; i < 100; i++) {
            builder.declarePosition();
            if (i % 2 == 0) {
                BIGINT.writeLong(builder.getBlockBuilder(0), i);
            }
            else {
                builder.getBlockBuilder(0).appendNull();
            }
        }
        Page input = builder.build();

        JavaAggregationFunctionImplementation function = functionAndTypeManager.getJavaAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("count", fromTypes()));
        AccumulatorFactory factory = generateAccumulatorFactory(function, ImmutableList.of(), Optional.empty());
        Accumulator accumulator = factory.createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(input);
        Block partialOutput = getIntermediateBlock(accumulator);

        AccumulatorFactory factoryBlockInput = generateAccumulatorFactory(function, ImmutableList.of(), Optional.empty());
        Accumulator accumulatorBlockInput = factoryBlockInput.createAccumulator(UpdateMemory.NOOP);
        accumulatorBlockInput.addBlockInput(input);
        Block partialOutputBlockInput = getIntermediateBlock(accumulatorBlockInput);

        assertEquals(partialOutput.getPositionCount(), partialOutputBlockInput.getPositionCount());
        assertEquals(partialOutput.getLong(0), partialOutputBlockInput.getLong(0));
    }

    @Test
    public void testSum()
    {
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT));

        for (int i = 0; i < 100; i++) {
            builder.declarePosition();
            if (i % 2 == 0) {
                BIGINT.writeLong(builder.getBlockBuilder(0), i);
            }
            else {
                builder.getBlockBuilder(0).appendNull();
            }
        }
        Page input = builder.build();

        JavaAggregationFunctionImplementation function = functionAndTypeManager.getJavaAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("sum", fromTypes(BIGINT)));
        AccumulatorFactory factory = generateAccumulatorFactory(function, ImmutableList.of(0), Optional.empty());
        Accumulator accumulator = factory.createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(input);
        Block partialOutput = getIntermediateBlock(accumulator);

        AccumulatorFactory factoryBlockInput = generateAccumulatorFactory(function, ImmutableList.of(0), Optional.empty());
        Accumulator accumulatorBlockInput = factoryBlockInput.createAccumulator(UpdateMemory.NOOP);
        accumulatorBlockInput.addBlockInput(input);
        Block partialOutputBlockInput = getIntermediateBlock(accumulatorBlockInput);

        assertEquals(partialOutput.getPositionCount(), partialOutputBlockInput.getPositionCount());
        assertEquals(partialOutput.getLong(0), partialOutputBlockInput.getLong(0));
    }

    @Test
    public void testApproxDistinct()
    {
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT));

        for (int i = 0; i < 100; i++) {
            builder.declarePosition();
            if (i % 2 == 0) {
                BIGINT.writeLong(builder.getBlockBuilder(0), i % 10);
            }
            else {
                builder.getBlockBuilder(0).appendNull();
            }
        }
        Page input = builder.build();

        JavaAggregationFunctionImplementation function = functionAndTypeManager.getJavaAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("approx_distinct", fromTypes(BIGINT)));
        AccumulatorFactory factory = generateAccumulatorFactory(function, ImmutableList.of(0), Optional.empty());
        Accumulator accumulator = factory.createAccumulator(UpdateMemory.NOOP);
        accumulator.addInput(input);
        Block partialOutput = getIntermediateBlock(accumulator);

        AccumulatorFactory factoryBlockInput = generateAccumulatorFactory(function, ImmutableList.of(0), Optional.empty());
        Accumulator accumulatorBlockInput = factoryBlockInput.createAccumulator(UpdateMemory.NOOP);
        accumulatorBlockInput.addBlockInput(input);
        Block partialOutputBlockInput = getIntermediateBlock(accumulatorBlockInput);

        assertEquals(partialOutput.getPositionCount(), partialOutputBlockInput.getPositionCount());
        assertEquals(partialOutput.getSlice(0, ((VariableWidthBlock) partialOutput).getPositionOffset(0), partialOutputBlockInput.getSliceLength(0)),
                partialOutputBlockInput.getSlice(0, ((VariableWidthBlock) partialOutputBlockInput).getPositionOffset(0), partialOutputBlockInput.getSliceLength(0)));
    }
}
