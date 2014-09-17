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
package com.facebook.presto.ml;

import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.CombineFunction;
import com.facebook.presto.operator.aggregation.InputFunction;
import com.facebook.presto.operator.aggregation.OutputFunction;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

import java.util.Locale;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

@AggregationFunction("evaluate_classifier_predictions")
public final class EvaluateClassifierPredictionsAggregation
{
    private EvaluateClassifierPredictionsAggregation() {}

    @InputFunction
    public static void input(EvaluateClassifierPredictionsState state, @SqlType(StandardTypes.BIGINT) long truth, @SqlType(StandardTypes.BIGINT) long prediction)
    {
        checkArgument(prediction == 1 || prediction == 0, "evaluate_predictions only supports binary classifiers");
        checkArgument(truth == 1 || truth == 0, "evaluate_predictions only supports binary classifiers");

        if (truth == 1) {
            if (prediction == 1) {
                state.setTruePositives(state.getTruePositives() + 1);
            }
            else {
                state.setFalseNegatives(state.getFalseNegatives() + 1);
            }
        }
        else {
            if (prediction == 0) {
                state.setTrueNegatives(state.getTrueNegatives() + 1);
            }
            else {
                state.setFalsePositives(state.getFalsePositives() + 1);
            }
        }
    }

    @CombineFunction
    public static void combine(EvaluateClassifierPredictionsState state, EvaluateClassifierPredictionsState scratchState)
    {
        state.setTruePositives(state.getTruePositives() + scratchState.getTruePositives());
        state.setFalsePositives(state.getFalsePositives() + scratchState.getFalsePositives());
        state.setTrueNegatives(state.getTrueNegatives() + scratchState.getTrueNegatives());
        state.setFalseNegatives(state.getFalseNegatives() + scratchState.getFalseNegatives());
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(EvaluateClassifierPredictionsState state, BlockBuilder out)
    {
        long truePositives = state.getTruePositives();
        long falsePositives = state.getFalsePositives();
        long trueNegatives = state.getTrueNegatives();
        long falseNegatives = state.getFalseNegatives();

        StringBuilder sb = new StringBuilder();
        long correct = trueNegatives + truePositives;
        long total = truePositives + trueNegatives + falsePositives + falseNegatives;
        sb.append(String.format(Locale.US, "Accuracy: %d/%d (%.2f%%)\n", correct, total, 100.0 * correct / (double) total));
        sb.append(String.format(Locale.US, "Precision: %d/%d (%.2f%%)\n", truePositives, truePositives + falsePositives, 100.0 * truePositives / (double) (truePositives + falsePositives)));
        sb.append(String.format(Locale.US, "Recall: %d/%d (%.2f%%)", truePositives, truePositives + falseNegatives, 100.0 * truePositives / (double) (truePositives + falseNegatives)));

        VARCHAR.writeString(out, sb.toString());
    }

    public interface EvaluateClassifierPredictionsState
            extends AccumulatorState
    {
        long getTruePositives();

        void setTruePositives(long value);

        long getFalsePositives();

        void setFalsePositives(long value);

        long getTrueNegatives();

        void setTrueNegatives(long value);

        long getFalseNegatives();

        void setFalseNegatives(long value);
    }
}
