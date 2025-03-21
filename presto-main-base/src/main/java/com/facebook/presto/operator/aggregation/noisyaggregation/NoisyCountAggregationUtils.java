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
import com.facebook.presto.util.SecureRandomGeneration;

import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class NoisyCountAggregationUtils
{
    private NoisyCountAggregationUtils()
    {
    }

    public static void updateState(NoisyCountState state, double noiseScale, Long randomSeed)
    {
        checkNoiseScale(noiseScale);

        // Update count and retain scale and random seed
        state.setCount(state.getCount() + 1);
        state.setNoiseScale(noiseScale);

        if (randomSeed == null) {
            state.setNullRandomSeed(true);
        }
        else {
            state.setNullRandomSeed(false);
            state.setRandomSeed(randomSeed);
        }
    }

    public static void combineStates(NoisyCountState state, NoisyCountState otherState)
    {
        state.setNoiseScale(state.getNoiseScale() > 0 ? state.getNoiseScale() : otherState.getNoiseScale()); // noise scale should be > 0
        checkNoiseScale(state.getNoiseScale());
        state.setCount(state.getCount() + otherState.getCount());

        if (state.isNullRandomSeed()) {
            state.setNullRandomSeed(otherState.isNullRandomSeed());
            state.setRandomSeed(otherState.getRandomSeed());
        }
    }

    public static void writeNoisyCountOutput(NoisyCountState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
            return;
        }

        double noise = getNoise(state);
        long noisyCount = computeNoisyCount(state, noise);
        BIGINT.writeLong(out, noisyCount);
    }

    public static double getNoise(NoisyCountState state)
    {
        Random random = SecureRandomGeneration.getNonBlocking();
        if (!state.isNullRandomSeed()) {
            random = new Random(state.getRandomSeed());
        }

        double noiseSdv = state.getNoiseScale();
        return random.nextGaussian() * noiseSdv;
    }

    public static void checkNoiseScale(double noiseScale)
    {
        if (noiseScale < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Noise scale must be >= 0");
        }
    }

    public static long computeNoisyCount(NoisyCountState state, double noise)
    {
        long trueCount = state.getCount();
        double noisyCount = trueCount + noise;
        double noisyCountFixedSign = Math.max(noisyCount, 0);  // count should always be >= 0
        return Math.round(noisyCountFixedSign);
    }
}
