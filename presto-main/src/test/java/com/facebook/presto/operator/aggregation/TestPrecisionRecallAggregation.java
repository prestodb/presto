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

import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class TestPrecisionRecallAggregation
        extends AbstractTestAggregationFunction
{
    private static final Integer NUM_BINS = 3;
    private static final double MIN_FALSE_PRED = 0.2;
    private static final double MAX_FALSE_PRED = 0.5;

    private final String functionName;
    private InternalAggregationFunction precisionRecallFunction;

    @BeforeClass
    public void setUp()
    {
        FunctionAndTypeManager functionAndTypeManager = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();
        precisionRecallFunction = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction(
                        this.functionName,
                        fromTypes(BIGINT, BOOLEAN, DOUBLE, DOUBLE)));
    }

    @Test
    public void testNegativeWeight()
    {
        try {
            assertAggregation(
                    precisionRecallFunction,
                    0.0,
                    createLongsBlock(Long.valueOf(200)),
                    createBooleansBlock(Boolean.valueOf(true)),
                    createDoublesBlock(Double.valueOf(0.2)),
                    createDoublesBlock(Double.valueOf(-0.2)));
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("weight"));
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("negative"));
        }
    }

    @Test
    public void testTooHighPrediction()
    {
        try {
            assertAggregation(
                    precisionRecallFunction,
                    0.0,
                    createLongsBlock(Long.valueOf(200)),
                    createBooleansBlock(Boolean.valueOf(true)),
                    createDoublesBlock(Double.valueOf(1.2)),
                    createDoublesBlock(Double.valueOf(0.2)));
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("prediction"));
        }
    }

    @Test
    public void testTooLowPrediction()
    {
        try {
            assertAggregation(
                    precisionRecallFunction,
                    0.0,
                    createLongsBlock(Long.valueOf(200)),
                    createBooleansBlock(Boolean.valueOf(true)),
                    createDoublesBlock(Double.valueOf(-1.2)),
                    createDoublesBlock(Double.valueOf(0.2)));
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("prediction"));
        }
    }

    @Test
    public void testNonConstantBuckets()
    {
        try {
            assertAggregation(
                    precisionRecallFunction,
                    0.0,
                    createLongsBlock(Long.valueOf(200), Long.valueOf(300)),
                    createBooleansBlock(Boolean.valueOf(true), Boolean.valueOf(false)),
                    createDoublesBlock(Double.valueOf(0.2), Double.valueOf(0.3)),
                    createDoublesBlock(Double.valueOf(1), Double.valueOf(1)));
            fail("Expected PrestoException");
        }
        catch (PrestoException e) {
            assertTrue(e.getMessage().toLowerCase(Locale.ENGLISH).contains("bucket"));
        }
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        start = Math.abs(start);

        BlockBuilder bucketCountBlockBuilder = BIGINT.createBlockBuilder(null, length);
        BlockBuilder outcomeBlockBuilder = BOOLEAN.createBlockBuilder(null, length);
        BlockBuilder predBlockBuilder = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BIGINT.writeLong(bucketCountBlockBuilder, TestPrecisionRecallAggregation.NUM_BINS);
            final Result result =
                    TestPrecisionRecallAggregation.getResult(start, length, i);
            BOOLEAN.writeBoolean(outcomeBlockBuilder, result.outcome);
            DOUBLE.writeDouble(predBlockBuilder, result.prediction);
        }

        return new Block[] {
                bucketCountBlockBuilder.build(),
                outcomeBlockBuilder.build(),
                predBlockBuilder.build(),
        };
    }

    private static class Result
    {
        public final Boolean outcome;
        public final Double prediction;

        public Result(Boolean outcome, Double prediction)
        {
            this.outcome = outcome;
            this.prediction = prediction;
        }
    }

    protected static class BucketResult
    {
        public final Double left;
        public final Double right;
        public final Double totalTrueWeight;
        public final Double totalFalseWeight;
        public final Double remainingTrueWeight;
        public final Double remainingFalseWeight;

        public BucketResult(
                Double left,
                Double right,
                Double totalTrueWeight,
                Double totalFalseWeight,
                Double remainingTrueWeight,
                Double remainingFalseWeight)
        {
            this.left = left;
            this.right = right;
            this.totalTrueWeight = totalTrueWeight;
            this.totalFalseWeight = totalFalseWeight;
            this.remainingTrueWeight = remainingTrueWeight;
            this.remainingFalseWeight = remainingFalseWeight;
        }
    }

    protected static Iterator<BucketResult> getResultsIterator(int start, int length)
    {
        final int effectiveStart = Math.abs(start);

        return new Iterator<BucketResult>()
        {
            int i;

            @Override
            public boolean hasNext()
            {
                final Double left = (double) (i) / TestPrecisionRecallAggregation.NUM_BINS;
                for (int j = start; j < effectiveStart + length; j++) {
                    final Result result =
                            TestPrecisionRecallAggregation.getResult(effectiveStart, length, j);
                    if (result.outcome && result.prediction >= left) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public BucketResult next()
            {
                final Double left = (double) (i) / TestPrecisionRecallAggregation.NUM_BINS;
                final Double right = (double) (i + 1) / TestPrecisionRecallAggregation.NUM_BINS;
                Double totalTrue = 0.0;
                Double totalFalse = 0.0;
                Double remainingTrue = 0.0;
                Double remainingFalse = 0.0;
                for (int j = start; j < start + length; j++) {
                    final Result result =
                            TestPrecisionRecallAggregation.getResult(start, length, j);
                    if (result.outcome) {
                        totalTrue += 1.0;
                        if (result.prediction >= left) {
                            remainingTrue += 1.0;
                        }
                    }
                    else {
                        totalFalse += 1.0;
                        if (result.prediction >= left) {
                            remainingFalse += 1.0;
                        }
                    }
                }
                i++;
                return new BucketResult(left, right, totalTrue, totalFalse, remainingTrue, remainingFalse);
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected TestPrecisionRecallAggregation(String functionName)
    {
        this.functionName = functionName;
    }

    @Override
    protected String getFunctionName()
    {
        return functionName;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(
                StandardTypes.INTEGER,
                StandardTypes.BOOLEAN,
                StandardTypes.DOUBLE);
    }

    protected static Result getResult(int start, int length, int i)
    {
        final Double prediction = Double.valueOf(i - start) / (length + 1);
        final Boolean outcome = prediction < TestPrecisionRecallAggregation.MIN_FALSE_PRED ||
                prediction > TestPrecisionRecallAggregation.MAX_FALSE_PRED;
        return new Result(outcome, prediction);
    }
}
