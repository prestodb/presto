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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.getIntermediateBlock;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRealHistogramAggregation
{
    private final AccumulatorFactory factory;
    private final Page input;

    public TestRealHistogramAggregation()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        InternalAggregationFunction function = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("numeric_histogram", fromTypes(BIGINT, REAL, DOUBLE)));
        factory = function.bind(ImmutableList.of(0, 1, 2), Optional.empty());
        input = makeInput(10);
    }

    @Test
    public void test()
    {
        Accumulator singleStep = factory.createAccumulator(UpdateMemory.NOOP);
        singleStep.addInput(input);
        Block expected = getFinalBlock(singleStep);

        Accumulator partialStep = factory.createAccumulator(UpdateMemory.NOOP);
        partialStep.addInput(input);
        Block partialBlock = getIntermediateBlock(partialStep);

        Accumulator finalStep = factory.createAccumulator(UpdateMemory.NOOP);
        finalStep.addIntermediate(partialBlock);
        Block actual = getFinalBlock(finalStep);

        assertEquals(extractSingleValue(actual), extractSingleValue(expected));
    }

    @Test
    public void testMerge()
    {
        Accumulator singleStep = factory.createAccumulator(UpdateMemory.NOOP);
        singleStep.addInput(input);
        Block singleStepResult = getFinalBlock(singleStep);

        Accumulator partialStep = factory.createAccumulator(UpdateMemory.NOOP);
        partialStep.addInput(input);
        Block intermediate = getIntermediateBlock(partialStep);

        Accumulator finalStep = factory.createAccumulator(UpdateMemory.NOOP);

        finalStep.addIntermediate(intermediate);
        finalStep.addIntermediate(intermediate);
        Block actual = getFinalBlock(finalStep);

        Map<Float, Float> expected = Maps.transformValues(extractSingleValue(singleStepResult), value -> value * 2);

        assertEquals(extractSingleValue(actual), expected);
    }

    @Test
    public void testNull()
    {
        Accumulator accumulator = factory.createAccumulator(UpdateMemory.NOOP);
        Block result = getFinalBlock(accumulator);

        assertTrue(result.getPositionCount() == 1);
        assertTrue(result.isNull(0));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testBadNumberOfBuckets()
    {
        Accumulator singleStep = factory.createAccumulator(UpdateMemory.NOOP);
        singleStep.addInput(makeInput(0));
        getFinalBlock(singleStep);
    }

    private static Map<Float, Float> extractSingleValue(Block block)
    {
        MapType mapType = mapType(REAL, REAL);
        return (Map<Float, Float>) mapType.getObjectValue(null, block, 0);
    }

    private static Page makeInput(int numberOfBuckets)
    {
        PageBuilder builder = new PageBuilder(ImmutableList.of(BIGINT, REAL, DOUBLE));

        for (int i = 0; i < 100; i++) {
            builder.declarePosition();

            BIGINT.writeLong(builder.getBlockBuilder(0), numberOfBuckets);
            REAL.writeLong(builder.getBlockBuilder(1), i); // value
            DOUBLE.writeDouble(builder.getBlockBuilder(2), 1); // weight
        }

        return builder.build();
    }
}
