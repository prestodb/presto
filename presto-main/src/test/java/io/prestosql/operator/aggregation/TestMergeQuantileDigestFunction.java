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
import io.airlift.stats.QuantileDigest;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.SqlVarbinary;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeParameter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.BiFunction;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.QuantileDigestParametricType.QDIGEST;
import static java.util.Objects.requireNonNull;

public class TestMergeQuantileDigestFunction
        extends AbstractTestAggregationFunction
{
    public static final BiFunction<Object, Object, Boolean> QDIGEST_EQUALITY = (actualBinary, expectedBinary) -> {
        if (actualBinary == null && expectedBinary == null) {
            return true;
        }
        requireNonNull(actualBinary, "actual value was null");
        requireNonNull(expectedBinary, "expected value was null");

        QuantileDigest actual = new QuantileDigest(wrappedBuffer(((SqlVarbinary) actualBinary).getBytes()));
        QuantileDigest expected = new QuantileDigest(wrappedBuffer(((SqlVarbinary) expectedBinary).getBytes()));
        return actual.getCount() == expected.getCount() &&
                actual.getMin() == expected.getMin() &&
                actual.getMax() == expected.getMax() &&
                actual.getAlpha() == expected.getAlpha() &&
                actual.getMaxError() == expected.getMaxError();
    };

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        Type type = QDIGEST.createType(typeRegistry, ImmutableList.of(TypeParameter.of(DoubleType.DOUBLE)));
        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            QuantileDigest qdigest = new QuantileDigest(0.0);
            qdigest.add(i);
            type.writeSlice(blockBuilder, qdigest.serialize());
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "merge";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("qdigest(double)");
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        QuantileDigest qdigest = new QuantileDigest(0.00);
        for (int i = start; i < start + length; i++) {
            qdigest.add(i);
        }
        return new SqlVarbinary(qdigest.serialize().getBytes());
    }

    // The following tests are overridden because by default simple equality checks are done, which often won't work with
    // qdigests due to the way they are serialized.  I am instead overridding these methods and using the QDIGEST_EQUALITY
    // function to perform equality checks.
    @Test
    @Override
    public void testMultiplePositions()
    {
        assertAggregation(getFunction(),
                QDIGEST_EQUALITY,
                "test multiple positions",
                new Page(getSequenceBlocks(0, 5)),
                getExpectedValue(0, 5));
    }

    @Test
    @Override
    public void testMixedNullAndNonNullPositions()
    {
        assertAggregation(getFunction(),
                QDIGEST_EQUALITY,
                "test mixed null and nonnull position",
                new Page(createAlternatingNullsBlock(getFunction().getParameterTypes(), getSequenceBlocks(0, 10))),
                getExpectedValueIncludingNulls(0, 10, 20));
    }
}
