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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.Page;
import com.google.common.base.Preconditions;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileAggregations.DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileAggregations.LONG_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileWeightedAggregations.DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileWeightedAggregations.LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public class TestApproximatePercentileAggregation
{
    @Test
    public void testLongPartialStep()
            throws Exception
    {
        // regular approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                null,
                createPage(
                        new Long[] {null},
                        0.5),
                createPage(
                        new Long[] {null},
                        0.5));

        assertAggregation(LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                1L,
                createPage(
                        new Long[] {null},
                        0.5),
                createPage(
                        new Long[] {1L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                2L,
                createPage(
                        new Long[] {null},
                        0.5),
                createPage(
                        new Long[] {1L, 2L, 3L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                2L,
                createPage(
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Long[] {2L, 3L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_AGGREGATION,
                3L,
                createPage(
                        new Long[] {1L, null, 2L, 2L, null, 2L, 2L, null},
                        0.5),
                createPage(
                        new Long[] {2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L},
                        0.5));

        // weighted approx_percentile
        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                null,
                createPage(
                        new Long[] {null},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Long[] {null},
                        new Long[] {1L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1L,
                createPage(
                        new Long[] {null},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Long[] {1L},
                        new Long[] {1L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2L,
                createPage(
                        new Long[] {null},
                        new Long[] {1L}, 0.5),
                createPage(
                        new Long[] {1L, 2L, 3L},
                        new Long[] {1L, 1L, 1L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2L,
                createPage(
                        new Long[] {1L},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Long[] {2L, 3L},
                        new Long[] {1L, 1L},
                        0.5));

        assertAggregation(
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                3L,
                createPage(
                        new Long[] {1L, null, 2L, null, 2L, null},
                        new Long[] {1L, 1L, 2L, 1L, 2L, 1L},
                        0.5),
                createPage(
                        new Long[] {2L, null, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L},
                        new Long[] {2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L},
                        0.5));
    }

    @Test
    public void testDoublePartialStep()
            throws Exception
    {
        // regular approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                null,
                createPage(
                        new Double[] {null},
                        0.5),
                createPage(
                        new Double[] {null},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                1.0,
                createPage(
                        new Double[] {null},
                        0.5),
                createPage(
                        new Double[] {1.0},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0,
                createPage(
                        new Double[] {null},
                        0.5),
                createPage(
                        new Double[] {1.0, 2.0, 3.0},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                2.0,
                createPage(
                        new Double[] {1.0},
                        0.5),
                createPage(
                        new Double[] {2.0, 3.0},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION,
                3.0,
                createPage(
                        new Double[] {1.0, null, 2.0, 2.0, null, 2.0, 2.0, null},
                        0.5),
                createPage(
                        new Double[] {2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0},
                        0.5));

        // weighted approx_percentile
        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                null,
                createPage(
                        new Double[] {null},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Double[] {null},
                        new Long[] {1L},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                1.0,
                createPage(
                        new Double[] {null},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Double[] {1.0},
                        new Long[] {1L},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0,
                createPage(
                        new Double[] {null},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Double[] {1.0, 2.0, 3.0},
                        new Long[] {1L, 1L, 1L},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                2.0,
                createPage(
                        new Double[] {1.0},
                        new Long[] {1L},
                        0.5),
                createPage(
                        new Double[] {2.0, 3.0},
                        new Long[] {1L, 1L},
                        0.5));

        assertAggregation(
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION,
                3.0,
                createPage(
                        new Double[] {1.0, null, 2.0, null, 2.0, null},
                        new Long[] {1L, 1L, 2L, 1L, 2L, 1L},
                        0.5),
                createPage(
                        new Double[] {2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0},
                        new Long[] {2L, 1L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L},
                        0.5));
    }

    private static Page createPage(Double[] values, double percentile)
    {
        Block valuesBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
        }
        else {
            valuesBlock = createDoublesBlock(values);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, percentilesBlock);
    }

    private static Page createPage(Long[] values, double percentile)
    {
        Block valuesBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, SINGLE_LONG, EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
        }
        else {
            valuesBlock = createLongsBlock(values);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, percentilesBlock);
    }

    private static Page createPage(Long[] values, Long[] weights, double percentile)
    {
        Preconditions.checkArgument(values.length == weights.length, "values.length must match weights.length");

        Block valuesBlock;
        Block weightsBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, SINGLE_LONG, EMPTY_SLICE);
            weightsBlock = new UncompressedBlock(0, SINGLE_LONG, EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
        }
        else {
            valuesBlock = createLongsBlock(values);
            weightsBlock = createLongsBlock(weights);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, weightsBlock, percentilesBlock);
    }

    private static Page createPage(Double[] values, Long[] weights, double percentile)
    {
        Preconditions.checkArgument(values.length == weights.length, "values.length must match weights.length");

        Block valuesBlock;
        Block weightsBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
            weightsBlock = new UncompressedBlock(0, SINGLE_LONG, EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, SINGLE_DOUBLE, EMPTY_SLICE);
        }
        else {
            valuesBlock = createDoublesBlock(values);
            weightsBlock = createLongsBlock(weights);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, weightsBlock, percentilesBlock);
    }
}
