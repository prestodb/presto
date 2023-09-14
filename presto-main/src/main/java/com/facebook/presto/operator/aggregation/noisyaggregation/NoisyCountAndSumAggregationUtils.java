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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountAggregationUtils.getNoise;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class NoisyCountAndSumAggregationUtils
{
    private NoisyCountAndSumAggregationUtils()
    {
    }

    public static void updateState(NoisyCountAndSumState state, double value, double noiseScale, Double lower, Double upper, Long randomSeed)
    {
        NoisyCountAggregationUtils.updateState(state, noiseScale, randomSeed);

        checkLowerUpper(lower, upper);
        double clippedValue = value;
        if (lower != null && upper != null) {
            state.setNullLowerUpper(false);
            state.setLower(lower);
            state.setUpper(upper);

            clippedValue = clip(value, lower, upper);
        }

        state.setSum(state.getSum() + clippedValue);
    }

    public static void combineStates(NoisyCountAndSumState state, NoisyCountAndSumState otherState)
    {
        NoisyCountAggregationUtils.combineStates(state, otherState);

        if (state.isNullLowerUpper()) {
            state.setNullLowerUpper(otherState.isNullLowerUpper());
            state.setLower(otherState.getLower());
            state.setUpper(otherState.getUpper());
        }

        state.setSum(state.getSum() + otherState.getSum());
    }

    public static void writeNoisySumOutput(NoisyCountAndSumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        double noise = getNoise(state);
        double trueSum = state.getSum();
        double noisySum = trueSum + noise;

        DOUBLE.writeDouble(out, noisySum);
    }

    public static void writeNoisyAvgOutput(NoisyCountAndSumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        double noise = getNoise(state);
        double trueAvg = state.getSum() / state.getCount();
        double noisyAvg = trueAvg + noise;

        DOUBLE.writeDouble(out, noisyAvg);
    }

    /**
     * Clip value to [lower, upper] range
     */
    public static double clip(double value, double lower, double upper)
    {
        return Math.max(lower, Math.min(upper, value));
    }

    public static void checkLowerUpper(Double lower, Double upper)
    {
        if (lower != null && upper != null) {
            if (upper < lower) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Lower must be <= upper");
            }
            return;
        }
        if (lower == null && upper == null) {
            return;
        }

        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Lower and upper should either both null or both non-null");
    }
}
