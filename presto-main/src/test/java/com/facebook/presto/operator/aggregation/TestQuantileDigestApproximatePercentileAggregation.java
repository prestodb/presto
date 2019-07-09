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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.tdigest.StatisticalDigestImplementation.QDIGEST;

public class TestQuantileDigestApproximatePercentileAggregation
        extends TestStatisticalDigestApproximatePercentileAggregation
{
    private static final FunctionManager functionManager = MetadataManager.createTestMetadataManager().getFunctionManager();

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION = getAggregation(functionManager, DOUBLE, DOUBLE);
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = getAggregation(functionManager, DOUBLE, BIGINT, DOUBLE);
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_PARAMETER = getAggregation(functionManager, DOUBLE, BIGINT, DOUBLE, DOUBLE);

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = getAggregation(functionManager, DOUBLE, new ArrayType(DOUBLE));
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = getAggregation(functionManager, DOUBLE, BIGINT, new ArrayType(DOUBLE));

    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_AGGREGATION = getAggregation(functionManager, BIGINT, DOUBLE);
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = getAggregation(functionManager, BIGINT, BIGINT, DOUBLE);
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = getAggregation(functionManager, BIGINT, BIGINT, DOUBLE, DOUBLE);

    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = getAggregation(functionManager, BIGINT, new ArrayType(DOUBLE));
    private static final InternalAggregationFunction LONG_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = getAggregation(functionManager, BIGINT, BIGINT, new ArrayType(DOUBLE));

    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_AGGREGATION = getAggregation(functionManager, REAL, DOUBLE);
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = getAggregation(functionManager, REAL, BIGINT, DOUBLE);
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_ACCURACY = getAggregation(functionManager, REAL, BIGINT, DOUBLE, DOUBLE);

    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = getAggregation(functionManager, REAL, new ArrayType(DOUBLE));
    private static final InternalAggregationFunction FLOAT_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = getAggregation(functionManager, REAL, BIGINT, new ArrayType(DOUBLE));

    private TestQuantileDigestApproximatePercentileAggregation()
    {
        super(new FeaturesConfig().setStatisticalDigestImplementation(QDIGEST));
    }

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

    @Override
    protected InternalAggregationFunction getInternalAggregationFunction(int arity)
    {
        switch (arity) {
            case 2:
                return DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION;
            case 3:
                return DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
            case 4:
                return DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_PARAMETER;
            default:
                throw new IllegalArgumentException("Unsupported number of arguments");
        }
    }

    @Override
    protected InternalAggregationFunction getArrayInternalAggregationFunction(int arity)
    {
        switch (arity) {
            case 2:
                return DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION;
            case 3:
                return DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION;
            default:
                throw new IllegalArgumentException("Unsupported number of arguments");
        }
    }
}
