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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.fixedhistogram.FixedDoubleHistogram;
import com.facebook.presto.operator.aggregation.state.PrecisionRecallState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.Streams;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public abstract class PrecisionRecallAggregation
{
    private static final double DEFAULT_WEIGHT = 1.0;
    private static final double MIN_PREDICTION_VALUE = 0.0;
    private static final double MAX_PREDICTION_VALUE = 1.0;
    // Effective maximum prediction, in order to ensure bin corresponding exactly to 1 is not reached.
    private static final double MAX_PREDICTION_VALUE_FOR_HISTOGRAM = 0.99999999999;
    private static final String ILLEGAL_PREDICTION_VALUE_MESSAGE = String.format(
            "Prediction value must be between %s and %s",
            MIN_PREDICTION_VALUE,
            MAX_PREDICTION_VALUE);
    private static final String NEGATIVE_WEIGHT_MESSAGE = "Weights must be non-negative";
    private static final String INCONSISTENT_BUCKET_COUNT_MESSAGE = "Bucket count must be constant";

    protected PrecisionRecallAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState PrecisionRecallState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double pred,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        if (state.getTrueWeights() == null) {
            state.setTrueWeights(new FixedDoubleHistogram(
                    (int) (bucketCount),
                    MIN_PREDICTION_VALUE,
                    MAX_PREDICTION_VALUE));
            state.setFalseWeights(new FixedDoubleHistogram(
                    (int) (bucketCount),
                    MIN_PREDICTION_VALUE,
                    MAX_PREDICTION_VALUE));
        }

        if (pred < MIN_PREDICTION_VALUE || pred > MAX_PREDICTION_VALUE) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    ILLEGAL_PREDICTION_VALUE_MESSAGE);
        }
        pred = Math.min(pred, MAX_PREDICTION_VALUE_FOR_HISTOGRAM);
        if (weight < 0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    NEGATIVE_WEIGHT_MESSAGE);
        }
        if (bucketCount != state.getTrueWeights().getBucketCount()) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    INCONSISTENT_BUCKET_COUNT_MESSAGE);
        }

        if (outcome) {
            state.getTrueWeights().add(pred, weight);
        }
        else {
            state.getFalseWeights().add(pred, weight);
        }
    }

    @InputFunction
    public static void input(
            @AggregationState PrecisionRecallState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double pred)
    {
        PrecisionRecallAggregation.input(state, bucketCount, outcome, pred, DEFAULT_WEIGHT);
    }

    @CombineFunction
    public static void combine(
            @AggregationState PrecisionRecallState state,
            @AggregationState PrecisionRecallState otherState)
    {
        if (state.getTrueWeights() == null && otherState.getTrueWeights() != null) {
            state.setTrueWeights(otherState.getTrueWeights().clone());
            state.setFalseWeights(otherState.getFalseWeights().clone());
            return;
        }
        if (state.getTrueWeights() != null && otherState.getTrueWeights() != null) {
            state.getTrueWeights().mergeWith(otherState.getTrueWeights());
            state.getFalseWeights().mergeWith(otherState.getFalseWeights());
        }
    }

    protected static class BucketResult
    {
        private final double threshold;
        private final double positive;
        private final double negative;
        private final double truePositive;
        private final double trueNegative;
        private final double falsePositive;
        private final double falseNegative;

        public double getThreshold()
        {
            return threshold;
        }

        public double getPositive()
        {
            return positive;
        }

        public double getNegative()
        {
            return negative;
        }

        public double getTruePositive()
        {
            return truePositive;
        }

        public double getTrueNegative()
        {
            return trueNegative;
        }

        public double getFalsePositive()
        {
            return falsePositive;
        }

        public double getFalseNegative()
        {
            return falseNegative;
        }

        public BucketResult(
                double threshold,
                double positive,
                double negative,
                double truePositive,
                double trueNegative,
                double falsePositive,
                double falseNegative)
        {
            this.threshold = threshold;
            this.positive = positive;
            this.negative = negative;
            this.truePositive = truePositive;
            this.trueNegative = trueNegative;
            this.falsePositive = falsePositive;
            this.falseNegative = falseNegative;
        }
    }

    protected static Iterator<BucketResult> getResultsIterator(@AggregationState PrecisionRecallState state)
    {
        if (state.getTrueWeights() == null) {
            return Collections.<BucketResult>emptyList().iterator();
        }

        double totalTrueWeight = Streams.stream(state.getTrueWeights().iterator())
                .mapToDouble(FixedDoubleHistogram.Bucket::getWeight)
                .sum();
        double totalFalseWeight = Streams.stream(state.getFalseWeights().iterator())
                .mapToDouble(FixedDoubleHistogram.Bucket::getWeight)
                .sum();

        return new Iterator<BucketResult>()
        {
            Iterator<FixedDoubleHistogram.Bucket> trueIterator = state.getTrueWeights().iterator();
            Iterator<FixedDoubleHistogram.Bucket> falseIterator = state.getFalseWeights().iterator();
            double runningFalseWeight;
            double runningTrueWeight;

            @Override
            public boolean hasNext()
            {
                return trueIterator.hasNext() && totalTrueWeight > runningTrueWeight;
            }

            @Override
            public BucketResult next()
            {
                if (!trueIterator.hasNext() || !falseIterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                FixedDoubleHistogram.Bucket trueResult = trueIterator.next();
                FixedDoubleHistogram.Bucket falseResult = falseIterator.next();

                BucketResult result = new BucketResult(
                        trueResult.getLeft(),
                        totalTrueWeight,
                        totalFalseWeight,
                        totalTrueWeight - runningTrueWeight,
                        runningFalseWeight,
                        totalFalseWeight - runningFalseWeight,
                        runningTrueWeight);

                runningTrueWeight += trueResult.getWeight();
                runningFalseWeight += falseResult.getWeight();

                return result;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
