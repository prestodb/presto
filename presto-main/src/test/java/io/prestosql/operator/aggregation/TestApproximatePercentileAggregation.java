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
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import static io.prestosql.block.BlockAssertions.createBlockOfReals;
import static io.prestosql.block.BlockAssertions.createDoubleSequenceBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createSequenceBlockOfReal;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public class TestApproximatePercentileAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, parseTypeSignature("array(double)"), DOUBLE.getTypeSignature(), parseTypeSignature("array(double)")));
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, parseTypeSignature("array(double)"), DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), parseTypeSignature("array(double)")));

    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, parseTypeSignature("array(bigint)"), BIGINT.getTypeSignature(), parseTypeSignature("array(double)")));
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, parseTypeSignature("array(bigint)"), BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), parseTypeSignature("array(double)")));

    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, REAL.getTypeSignature(),
                    ImmutableList.of(REAL.getTypeSignature(), DOUBLE.getTypeSignature())));
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, REAL.getTypeSignature(),
                    ImmutableList.of(REAL.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature())));
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, REAL.getTypeSignature(),
                    ImmutableList.of(REAL.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature())));

    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, new ArrayType(REAL).getTypeSignature(),
                    ImmutableList.of(REAL.getTypeSignature(), new ArrayType(DOUBLE).getTypeSignature())));
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("approx_percentile", AGGREGATE, new ArrayType(REAL).getTypeSignature(),
                    ImmutableList.of(REAL.getTypeSignature(), BIGINT.getTypeSignature(), new ArrayType(DOUBLE).getTypeSignature())));

    @Test
    public void testLongPartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createRLEBlock(0.5, 2));

        assertAggregation(LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1L,
                createLongsBlock(null, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                3L,
                createLongsBlock(1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createRLEBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 2));

        assertAggregation(LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1L, 1L),
                createLongsBlock(null, 1L),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1L, 2L, 3L),
                createLongsBlock(null, 1L, 2L, 3L),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(2L, 3L),
                createLongsBlock(1L, 2L, 3L),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1L, 3L),
                createLongsBlock(1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21));

        // weighted approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1L,
                createLongsBlock(null, 1L),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                3L,
                createLongsBlock(1L, null, 2L, null, 2L, null, 2L, null, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 17));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                9900L,
                createLongSequenceBlock(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.99, 10000),
                createRLEBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION,
                ImmutableList.of(1L, 2L),
                createLongsBlock(1L, 2L, 3L),
                createLongsBlock(4L, 2L, 1L),
                createRLEBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    @Test
    public void testFloatPartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0f,
                createBlockOfReals(null, 1.0f),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0f,
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createRLEBlock(0.5, 4));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0f,
                createBlockOfReals(-1.0f, 1.0f),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                -1.0f,
                createBlockOfReals(-2.0f, 3.0f, -1.0f),
                createRLEBlock(0.5, 3));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0f,
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createRLEBlock(0.5, 3));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION,
                3.0f,
                createBlockOfReals(1.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 3.0f, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createRLEBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0f, 1.0f),
                createBlockOfReals(null, 1.0f),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0f, 2.0f, 3.0f),
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(2.0f, 3.0f),
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0f, 3.0f),
                createBlockOfReals(1.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 3.0f, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21));

        // weighted approx_percentile
        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0f,
                createBlockOfReals(null, 1.0f),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0f,
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0f,
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                3.0f,
                createBlockOfReals(1.0f, null, 2.0f, null, 2.0f, null, 2.0f, null, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 17));

        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                9900.0f,
                createSequenceBlockOfReal(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.99, 10000),
                createRLEBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION,
                ImmutableList.of(1.0f, 2.0f),
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createLongsBlock(4L, 2L, 1L),
                createRLEBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    @Test
    public void testDoublePartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                createDoublesBlock(null, 1.0),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(0.5, 3));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                3.0,
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createRLEBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0, 1.0),
                createDoublesBlock(null, 1.0),
                createRLEBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0, 2.0, 3.0),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createRLEBlock(ImmutableList.of(0.01, 0.5), 21));

        // weighted approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                createDoublesBlock(null, 1.0),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                3.0,
                createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 17));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                9900.0,
                createDoubleSequenceBlock(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.99, 10000),
                createRLEBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION,
                ImmutableList.of(1.0, 2.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createLongsBlock(4L, 2L, 1L),
                createRLEBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    private static RunLengthEncodedBlock createRLEBlock(double percentile, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, percentile);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    private static RunLengthEncodedBlock createRLEBlock(Iterable<Double> percentiles, int positionCount)
    {
        BlockBuilder rleBlockBuilder = new ArrayType(DOUBLE).createBlockBuilder(null, 1);
        BlockBuilder arrayBlockBuilder = rleBlockBuilder.beginBlockEntry();

        for (double percentile : percentiles) {
            DOUBLE.writeDouble(arrayBlockBuilder, percentile);
        }

        rleBlockBuilder.closeEntry();

        return new RunLengthEncodedBlock(rleBlockBuilder.build(), positionCount);
    }
}
