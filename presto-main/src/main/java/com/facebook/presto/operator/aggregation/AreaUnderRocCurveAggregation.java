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

import com.facebook.presto.operator.aggregation.state.AreaUnderRocCurveState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

@AggregationFunction("area_under_roc_curve")
public final class AreaUnderRocCurveAggregation
{
    private AreaUnderRocCurveAggregation() {}

    @InputFunction
    public static void input(@AggregationState AreaUnderRocCurveState state, @SqlType(StandardTypes.BOOLEAN) boolean label, @SqlType(StandardTypes.DOUBLE) double score)
    {
        MultiKeyValuePairs pairs = state.get();
        if (pairs == null) {
            pairs = new MultiKeyValuePairs(BOOLEAN, DOUBLE);
            state.set(pairs);
        }

        BlockBuilder labelBlockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1);
        BOOLEAN.writeBoolean(labelBlockBuilder, label);

        BlockBuilder scoreBlockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
        DOUBLE.writeDouble(scoreBlockBuilder, score);

        long startSize = pairs.estimatedInMemorySize();
        pairs.add(labelBlockBuilder.build(), scoreBlockBuilder.build(), 0, 0);
        state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
    }

    @CombineFunction
    public static void combine(@AggregationState AreaUnderRocCurveState state, @AggregationState AreaUnderRocCurveState otherState)
    {
        if (state.get() != null && otherState.get() != null) {
            Block labelsBlock = otherState.get().getKeys();
            Block scoresBlock = otherState.get().getValues();
            MultiKeyValuePairs pairs = state.get();
            long startSize = pairs.estimatedInMemorySize();
            for (int i = 0; i < labelsBlock.getPositionCount(); i++) {
                pairs.add(labelsBlock, scoresBlock, i, i);
            }
            state.addMemoryUsage(pairs.estimatedInMemorySize() - startSize);
        }
        else if (state.get() == null) {
            state.set(otherState.get());
        }
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState AreaUnderRocCurveState state, BlockBuilder out)
    {
        MultiKeyValuePairs pairs = state.get();
        if (pairs == null) {
            out.appendNull();
        }
        else {
            Block labelsBlock = pairs.getKeys();
            Block scoresBlock = pairs.getValues();
            boolean[] labels = new boolean[labelsBlock.getPositionCount()];
            double[] scores = new double[labelsBlock.getPositionCount()];
            for (int i = 0; i < labelsBlock.getPositionCount(); i++) {
                labels[i] = BOOLEAN.getBoolean(labelsBlock, i);
                scores[i] = DOUBLE.getDouble(scoresBlock, i);
            }
            double auc = computeAUC(labels, scores);
            if (Double.isFinite(auc)) {
                DOUBLE.writeDouble(out, auc);
            }
            else {
                out.appendNull();
            }
        }
    }

    /**
     * Calculate Area Under the ROC Curve using trapezoidal rule
     * See Algorithm 4 in a paper "ROC Graphs: Notes and Practical Considerations
     * for Data Mining Researchers" for detail:
     * http://www.hpl.hp.com/techreports/2003/HPL-2003-4.pdf
     */
    private static double computeAUC(boolean[] labels, double[] scores)
    {
        // labels sorted descending by scores
        int[] indices = new int[labels.length];
        for (int i = 0; i < labels.length; i++) {
            indices[i] = i;
        }
        IntArrays.quickSort(indices, new AbstractIntComparator()
        {
            @Override
            public int compare(int i, int j)
            {
                return Double.compare(scores[j], scores[i]);
            }
        });

        int truePositive = 0;
        int falsePositive = 0;

        int truePositivePrev = 0;
        int falsePositivePrev = 0;

        double area = 0.0;

        double scorePrev = Double.POSITIVE_INFINITY;

        for (int i : indices) {
            if (Double.compare(scores[i], scorePrev) != 0) {
                area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

                scorePrev = scores[i];
                falsePositivePrev = falsePositive;
                truePositivePrev = truePositive;
            }
            if (labels[i]) {
                truePositive++;
            }
            else {
                falsePositive++;
            }
        }

        // AUC for single class outcome is not defined
        if (truePositive == 0 || falsePositive == 0) {
            return Double.POSITIVE_INFINITY;
        }

        // finalize by adding a trapezoid area based on the final tp/fp counts
        area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

        return area / (truePositive * falsePositive); // scaled value in the 0-1 range
    }

    /**
     * Calculate trapezoidal area under (x1, y1)-(x2, y2)
     */
    private static double trapezoidArea(double x1, double x2, double y1, double y2)
    {
        double base = Math.abs(x1 - x2);
        double averageHeight = (y1 + y2) / 2;
        return base * averageHeight;
    }
}
