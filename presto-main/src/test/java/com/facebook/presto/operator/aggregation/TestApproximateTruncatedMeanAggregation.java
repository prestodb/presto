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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;

public class TestApproximateTruncatedMeanAggregation
{
    private static final FunctionRegistry functionRegistry = MetadataManager.createTestMetadataManager().getFunctionRegistry();

    private static final InternalAggregationFunction DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    DOUBLE.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction LONG_APPROX_TRUNCATED_MEAN_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, DOUBLE.getTypeSignature(),
                    BIGINT.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, REAL.getTypeSignature(),
                    REAL.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, REAL.getTypeSignature(),
                    REAL.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    private static final InternalAggregationFunction FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY = functionRegistry.getAggregateFunctionImplementation(
            new Signature("approx_truncated_mean", AGGREGATE, REAL.getTypeSignature(),
                    REAL.getTypeSignature(), BIGINT.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));

    @Test
    public void testLongPartialStep()
    {
        // regular approx_truncated_mean
        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createLongsBlock(null, 1L),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.0,
                createLongsBlock(null, 1L, 2L, 3L),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.5,
                createLongsBlock(1L, 2L, 3L),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.5,
                createLongsBlock(1L, 2L, 5L, 8L, 9L),
                createRLEBlock(0.0, 5),
                createRLEBlock(0.5, 5));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                2.5,
                createLongsBlock(1L, 2L, 3L, 8L, 9L),
                createRLEBlock(0.3, 5),
                createRLEBlock(0.7, 5));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                2.0,
                createLongsBlock(1L, 2L, 3L, 57L),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.95, 4));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.5,
                createLongsBlock(1L, 2L, 57L),
                createRLEBlock(0.0, 3),
                createRLEBlock(1.0, 3));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                20.0,
                createLongsBlock(1L, 2L, 57L, 58L),
                createRLEBlock(0.0, 4),
                createRLEBlock(1.0, 4));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createLongsBlock(1L, 10L, 30L),
                createRLEBlock(0.49, 3),
                createRLEBlock(0.5, 3));

        // weighted approx_truncated_mean
        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createLongsBlock(null, null),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createLongsBlock(null, 1L),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                1.0,
                createLongsBlock(null, 1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                1.5,
                createLongsBlock(1L, 2L, 3L),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                1.0,
                createLongsBlock(1L, 2L, 50L, 60L, 70L, null),
                createLongsBlock(1L, 10L, 1L, 2L, 2L, 5L),
                createRLEBlock(0.0, 6),
                createRLEBlock(0.3, 6));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                5000.0,
                createLongSequenceBlock(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.1, 10000),
                createRLEBlock(0.9001, 10000),
                createRLEBlock(0.001, 10000));

        assertAggregation(
                LONG_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                0.0,
                createLongSequenceBlock(-5000, 5000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.1, 10000),
                createRLEBlock(0.9001, 10000),
                createRLEBlock(0.001, 10000));
    }

    @Test
    public void testDoublePartialStep()
    {
        // regular approx_truncated_mean
        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createDoublesBlock(null, 1.0),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.0,
                createDoublesBlock(null, 1.0, 2.6, 3.0),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.8,
                createDoublesBlock(1.0, 2.6, 3.0),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.5,
                createDoublesBlock(0.5, 2.5, 5.0, 8.0, 9.9),
                createRLEBlock(0.0, 5),
                createRLEBlock(0.5, 5));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                2.9,
                createDoublesBlock(0.5, 2.5, 3.3, 8.0, 9.0),
                createRLEBlock(0.3, 5),
                createRLEBlock(0.7, 5));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                0.5,
                createDoublesBlock(0.1, 0.4, 1.0, 57.0),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.95, 4));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.5,
                createDoublesBlock(1.0, 2.0, 57.0),
                createRLEBlock(0.0, 3),
                createRLEBlock(1.0, 3));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                20.1,
                createDoublesBlock(1.0, 2.0, 57.3, 58.0),
                createRLEBlock(0.0, 4),
                createRLEBlock(1.0, 4));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createDoublesBlock(1.0, 10.0, 30.0),
                createRLEBlock(0.49, 3),
                createRLEBlock(0.5, 3));

        // weighted approx_truncated_mean
        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(1.0, 1.0),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createDoublesBlock(null, 1.0),
                createDoublesBlock(1.0, 1.0),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                1.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createDoublesBlock(1.0, 1.0, 1.0, 1.0),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                2.0,
                createDoublesBlock(1.5, 2.5, 3.0),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                0.3,
                createDoublesBlock(0.3, 2.0, 50.0, 60.0, 70.0, null),
                createLongsBlock(1L, 10L, 1L, 2L, 2L, 5L),
                createRLEBlock(0.0, 6),
                createRLEBlock(0.3, 6));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                5000.0,
                createDoubleSequenceBlock(0, 10000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.1, 10000),
                createRLEBlock(0.9001, 10000),
                createRLEBlock(0.001, 10000));

        assertAggregation(
                DOUBLE_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                0.0,
                createDoubleSequenceBlock(-5000, 5000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.1, 10000),
                createRLEBlock(0.9001, 10000),
                createRLEBlock(0.001, 10000));
    }

    @Test
    public void testFloatPartialStep()
    {
        // regular approx_truncated_mean
        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                null,
                createBlockOfReals(null, 1.0f),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.0f,
                createBlockOfReals(null, 1.0f, 2.6f, 3.0f),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                0.8f,
                createBlockOfReals(-1.0f, 2.6f, 3.0f),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                1.0f,
                createBlockOfReals(-0.5f, 2.5f, 5.0f, 8.3f, 9.9f),
                createRLEBlock(0.0, 5),
                createRLEBlock(0.5, 5));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_AGGREGATION,
                0.4f,
                createBlockOfReals(-0.2f, 0.4f, 1.0f, 57.0f),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.95, 4));

        // weighted approx_truncated_mean
        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createBlockOfReals(null, null),
                createBlockOfReals(1.1f, 1.0f),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                null,
                createBlockOfReals(null, 1.0f),
                createLongsBlock(1L, 1L),
                createRLEBlock(0.0, 2),
                createRLEBlock(0.5, 2));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                -1.0f,
                createBlockOfReals(null, -1.0f, 2.0f, 3.0f),
                createLongsBlock(1L, 1L, 1L, 1L),
                createRLEBlock(0.0, 4),
                createRLEBlock(0.5, 4));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                2.0f,
                createBlockOfReals(1.5f, 2.5f, 3.0f),
                createLongsBlock(1L, 1L, 1L),
                createRLEBlock(0.0, 3),
                createRLEBlock(0.9, 3));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION,
                0.3f,
                createBlockOfReals(0.3f, 2.0f, 50.0f, 60.0f, 70.0f, null),
                createLongsBlock(1L, 10L, 1L, 2L, 2L, 5L),
                createRLEBlock(0.0, 6),
                createRLEBlock(0.3, 6));

        assertAggregation(
                FLOAT_APPROX_TRUNCATED_MEAN_WEIGHTED_AGGREGATION_WITH_ACCURACY,
                0.0f,
                createSequenceBlockOfReal(-5000, 5000),
                createLongRepeatBlock(1, 10000),
                createRLEBlock(0.1, 10000),
                createRLEBlock(0.9001, 10000),
                createRLEBlock(0.001, 10000));
    }

    private static RunLengthEncodedBlock createRLEBlock(double value, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, value);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }
}
