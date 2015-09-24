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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations.DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations.DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.LONG_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations.LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestApproximatePercentileAggregation
{
    private static final Block EMPTY_DOUBLE_BLOCK = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 0).build();
    private static final Block EMPTY_LONG_BLOCK = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 0).build();

    @Test
    public void testLongPartialStep()
            throws Exception
    {
        // regular approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                null,
                createLongsBlock(null, null),
                createRLEBlock(0.5, 2));

        assertAggregation(LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                1L,
                createLongsBlock(null, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                3L,
                createLongsBlock(1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createRLEBlock(0.5, 21));

        // weighted approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                null,
                createLongsBlock(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                1L,
                createLongsBlock(null, 1L),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                3L,
                createLongsBlock(1L, null, 2L, null, 2L, null, 2L, null, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 17));
    }

    @Test
    public void testDoublePartialStep()
            throws Exception
    {
        // regular approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                1.0,
                createDoublesBlock(null, 1.0),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRLEBlock(0.5, 3));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                3.0,
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createRLEBlock(0.5, 21));

        // weighted approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                null,
                createDoublesBlock(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                1.0,
                createDoublesBlock(null, 1.0),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.5, 3));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                3.0,
                createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createLongsBlock(1L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
                createRLEBlock(0.5, 17));
    }
}
