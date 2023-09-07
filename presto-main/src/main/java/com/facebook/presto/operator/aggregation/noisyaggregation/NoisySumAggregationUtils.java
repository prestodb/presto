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

import java.security.SecureRandom;
import java.util.Random;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class NoisySumAggregationUtils
{
    private NoisySumAggregationUtils()
    {
    }

    public static void updateNoisySumState(NoisySumState state, double value, double noiseScale, Double lower, Double upper, Long randomSeed)
    {
        state.setNoiseScale(noiseScale);
        checkNoiseScale(state.getNoiseScale());

        state.setCount(state.getCount() + 1);

        if (randomSeed == null) {
            state.setNullRandomSeed(true);
        }
        else {
            state.setNullRandomSeed(false);
            state.setRandomSeed(randomSeed);
        }

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

    public static void combineNoisySumStates(NoisySumState state, NoisySumState otherState)
    {
        state.setNoiseScale(state.getNoiseScale() > 0 ? state.getNoiseScale() : otherState.getNoiseScale()); // noise scale should be > 0
        checkNoiseScale(state.getNoiseScale());
        if (state.isNullLowerUpper()) {
            state.setNullLowerUpper(otherState.isNullLowerUpper());
            state.setLower(otherState.getLower());
            state.setUpper(otherState.getUpper());
        }

        state.setCount(state.getCount() + otherState.getCount());

        if (state.isNullRandomSeed()) {
            state.setNullRandomSeed(otherState.isNullRandomSeed());
            state.setRandomSeed(otherState.getRandomSeed());
        }

        state.setSum(state.getSum() + otherState.getSum());
    }

    public static void writeNoisySumStateOutput(NoisySumState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        Random random = new SecureRandom();
        if (!state.isNullRandomSeed()) {
            random = new Random(state.getRandomSeed());
        }
        // add noise
        double noiseSdv = state.getNoiseScale();
        double noise = random.nextGaussian() * noiseSdv;
        double trueSum = state.getSum();
        double noisySum = trueSum + noise;

        DOUBLE.writeDouble(out, noisySum);
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

    public static void checkNoiseScale(double noiseScale)
    {
        if (noiseScale < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Noise scale must be >= 0");
        }
    }
}
