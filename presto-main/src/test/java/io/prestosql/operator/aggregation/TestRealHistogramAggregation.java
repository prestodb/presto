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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.operator.aggregation.AggregationTestUtils.getFinalBlock;
import static io.prestosql.operator.aggregation.AggregationTestUtils.getIntermediateBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRealHistogramAggregation
{
    private final AccumulatorFactory factory;
    private final Page input;

    public TestRealHistogramAggregation()
    {
        TypeRegistry typeRegistry = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        InternalAggregationFunction function = functionRegistry.getAggregateFunctionImplementation(
                new Signature("numeric_histogram",
                        AGGREGATE,
                        parseTypeSignature("map(real, real)"),
                        parseTypeSignature(StandardTypes.BIGINT),
                        parseTypeSignature(StandardTypes.REAL),
                        parseTypeSignature(StandardTypes.DOUBLE)));
        factory = function.bind(ImmutableList.of(0, 1, 2), Optional.empty());

        input = makeInput(10);
    }

    @Test
    public void test()
    {
        Accumulator singleStep = factory.createAccumulator();
        singleStep.addInput(input);
        Block expected = getFinalBlock(singleStep);

        Accumulator partialStep = factory.createAccumulator();
        partialStep.addInput(input);
        Block partialBlock = getIntermediateBlock(partialStep);

        Accumulator finalStep = factory.createAccumulator();
        finalStep.addIntermediate(partialBlock);
        Block actual = getFinalBlock(finalStep);

        assertEquals(extractSingleValue(actual), extractSingleValue(expected));
    }

    @Test
    public void testMerge()
    {
        Accumulator singleStep = factory.createAccumulator();
        singleStep.addInput(input);
        Block singleStepResult = getFinalBlock(singleStep);

        Accumulator partialStep = factory.createAccumulator();
        partialStep.addInput(input);
        Block intermediate = getIntermediateBlock(partialStep);

        Accumulator finalStep = factory.createAccumulator();

        finalStep.addIntermediate(intermediate);
        finalStep.addIntermediate(intermediate);
        Block actual = getFinalBlock(finalStep);

        Map<Float, Float> expected = Maps.transformValues(extractSingleValue(singleStepResult), value -> value * 2);

        assertEquals(extractSingleValue(actual), expected);
    }

    @Test
    public void testNull()
    {
        Accumulator accumulator = factory.createAccumulator();
        Block result = getFinalBlock(accumulator);

        assertTrue(result.getPositionCount() == 1);
        assertTrue(result.isNull(0));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testBadNumberOfBuckets()
    {
        Accumulator singleStep = factory.createAccumulator();
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
