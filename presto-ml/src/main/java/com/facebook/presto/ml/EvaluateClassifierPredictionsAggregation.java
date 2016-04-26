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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Sets.union;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.String.format;

@AggregationFunction("evaluate_classifier_predictions")
public final class EvaluateClassifierPredictionsAggregation
{
    private EvaluateClassifierPredictionsAggregation() {}

    @InputFunction
    public static void input(EvaluateClassifierPredictionsState state, @SqlType(StandardTypes.BIGINT) long truth, @SqlType(StandardTypes.BIGINT) long prediction)
    {
        input(state, Slices.utf8Slice(String.valueOf(truth)), Slices.utf8Slice(String.valueOf(prediction)));
    }

    @InputFunction
    @LiteralParameters({"x", "y"})
    public static void input(EvaluateClassifierPredictionsState state, @SqlType("varchar(x)") Slice truth, @SqlType("varchar(y)") Slice prediction)
    {
        if (truth.equals(prediction)) {
            String key = truth.toStringUtf8();
            if (!state.getTruePositives().containsKey(key)) {
                state.addMemoryUsage(truth.length() + SIZE_OF_INT);
            }
            state.getTruePositives().put(key, state.getTruePositives().getOrDefault(key, 0) + 1);
        }
        else {
            String truthKey = truth.toStringUtf8();
            String predictionKey = prediction.toStringUtf8();
            if (!state.getFalsePositives().containsKey(predictionKey)) {
                state.addMemoryUsage(prediction.length() + SIZE_OF_INT);
            }
            state.getFalsePositives().put(predictionKey, state.getFalsePositives().getOrDefault(predictionKey, 0) + 1);
            if (!state.getFalseNegatives().containsKey(truthKey)) {
                state.addMemoryUsage(truth.length() + SIZE_OF_INT);
            }
            state.getFalseNegatives().put(truthKey, state.getFalseNegatives().getOrDefault(truthKey, 0) + 1);
        }
    }

    @CombineFunction
    public static void combine(EvaluateClassifierPredictionsState state, EvaluateClassifierPredictionsState scratchState)
    {
        int size = 0;
        size += mergeMaps(state.getTruePositives(), scratchState.getTruePositives());
        size += mergeMaps(state.getFalsePositives(), scratchState.getFalsePositives());
        size += mergeMaps(state.getFalseNegatives(), scratchState.getFalseNegatives());
        state.addMemoryUsage(size);
    }

    // Returns the estimated memory increase in map
    private static int mergeMaps(Map<String, Integer> map, Map<String, Integer> other)
    {
        int deltaSize = 0;
        for (Map.Entry<String, Integer> entry : other.entrySet()) {
            if (!map.containsKey(entry.getKey())) {
                deltaSize += entry.getKey().getBytes().length + SIZE_OF_INT;
            }
            map.put(entry.getKey(), map.getOrDefault(entry.getKey(), 0) + other.getOrDefault(entry.getKey(), 0));
        }
        return deltaSize;
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(EvaluateClassifierPredictionsState state, BlockBuilder out)
    {
        StringBuilder sb = new StringBuilder();
        long correct = state.getTruePositives()
                .values()
                .stream()
                .reduce(0, Integer::sum);
        long total = correct + state.getFalsePositives().values().stream().reduce(0, Integer::sum);
        sb.append(format(Locale.US, "Accuracy: %d/%d (%.2f%%)\n", correct, total, 100.0 * correct / (double) total));
        Set<String> labels = union(union(state.getTruePositives().keySet(), state.getFalsePositives().keySet()), state.getFalseNegatives().keySet());
        for (String label : labels) {
            int truePositives = state.getTruePositives().getOrDefault(label, 0);
            int falsePositives = state.getFalsePositives().getOrDefault(label, 0);
            int falseNegatives = state.getFalseNegatives().getOrDefault(label, 0);
            sb.append(format(Locale.US, "Class '%s'\n", label));
            sb.append(format(Locale.US, "Precision: %d/%d (%.2f%%)\n", truePositives, truePositives + falsePositives, 100.0 * truePositives / (double) (truePositives + falsePositives)));
            sb.append(format(Locale.US, "Recall: %d/%d (%.2f%%)\n", truePositives, truePositives + falseNegatives, 100.0 * truePositives / (double) (truePositives + falseNegatives)));
        }

        VARCHAR.writeString(out, sb.toString());
    }
}
