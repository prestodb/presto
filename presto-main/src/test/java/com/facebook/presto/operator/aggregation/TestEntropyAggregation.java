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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertTrue;

public class TestEntropyAggregation
        extends AbstractTestAggregationFunction
{
    private static final String FUNCTION_NAME = "entropy";

    private InternalAggregationFunction entropyFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionAndTypeManager functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
        entropyFunction = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction(TestEntropyAggregation.FUNCTION_NAME, fromTypes(BIGINT)));
    }

    @Test
    public void entropyOfASingle()
    {
        assertAggregation(entropyFunction,
                0.0,
                createLongsBlock(Long.valueOf(1)));
    }

    @Test
    public void entropyOfTwoEquals()
    {
        Long x;

        x = Long.valueOf(1);
        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(x, x));

        x = Long.valueOf(20);
        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(x, x));
    }

    @Test
    public void entropyOfTwoEqualsWithNulls()
    {
        Long x;

        x = Long.valueOf(1);
        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(x, null, x));

        x = Long.valueOf(10);
        assertAggregation(entropyFunction,
                1.0,
                createLongsBlock(null, null, x, x));
    }

    @Test
    public void entropyOfTwoSkewed()
    {
        Long lower = Long.valueOf(30);
        Long higher = Long.valueOf(70);
        Double expected = 0.8812908992306931;

        assertAggregation(entropyFunction,
                expected,
                createLongsBlock(lower, higher));
        assertAggregation(entropyFunction,
                expected,
                createLongsBlock(higher, lower));
    }

    @Test
    public void entropyOfOnlyNulls()
    {
        assertAggregation(entropyFunction,
                0.0,
                createLongsBlock(null, null));
    }

    @Test
    public void entropyOfNegativeCount()
    {
        String error = "";
        try {
            assertAggregation(entropyFunction,
                    0.0,
                    createLongsBlock(Long.valueOf(-1)));
        }
        catch (PrestoException e) {
            error = e.getMessage();
        }
        assertTrue(error.toLowerCase(Locale.ENGLISH).contains("negative"));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(blockBuilder, Math.abs(i));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        final ArrayList<Integer> counts = IntStream
                .range(start, start + length)
                .map(c -> Math.abs(c))
                .boxed()
                .collect(Collectors.toCollection(ArrayList::new));
        if (counts.stream().anyMatch(c -> c < 0)) {
            return null;
        }
        final double sum = counts.stream()
                .mapToDouble(c -> Math.max(c, 0.0))
                .sum();
        if (sum == 0) {
            return 0.0;
        }
        final ArrayList<Double> entropies = counts.stream()
                .filter(c -> c > 0)
                .map(c -> (c / sum) * Math.log(sum / c))
                .collect(Collectors.toCollection(ArrayList::new));
        return entropies.isEmpty() ?
                0 :
                entropies.stream().mapToDouble(c -> c).sum() / Math.log(2);
    }

    @Override
    protected String getFunctionName()
    {
        return TestEntropyAggregation.FUNCTION_NAME;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.INTEGER);
    }
}
