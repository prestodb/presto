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

package com.facebook.presto.hudi.split;

import com.facebook.presto.spi.SplitWeight;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Doubles.constrainToRange;
import static java.util.Objects.requireNonNull;

/**
 * Hudi split weight provider based on the split size.
 *
 * The standard split size, `standardSplitSize` in bytes, corresponding split weight "1" and the minimum weight
 * `minimumWeight` are given during the initialization.
 *
 * Given the split size in bytes as `splitSizeInBytes`, the raw weight is calculated as
 * `splitSizeInBytes / standardSplitSize`.  If the weight is smaller than `minimumWeight`, `minimumWeight` is used.
 * If the weight is larger than 1, 1 is used as the weight.  The split weight is always in the range of
 * [`minimumWeight`, 1].
 */
public class SizeBasedSplitWeightProvider
        implements HudiSplitWeightProvider
{
    private final double minimumWeight;
    private final double standardSplitSizeInBytes;

    public SizeBasedSplitWeightProvider(double minimumWeight, DataSize standardSplitSize)
    {
        checkArgument(
                Double.isFinite(minimumWeight) && minimumWeight > 0 && minimumWeight <= 1,
                "minimumWeight must be > 0 and <= 1, found: %s", minimumWeight);
        this.minimumWeight = minimumWeight;
        long standardSplitSizeInBytesLong = requireNonNull(standardSplitSize, "standardSplitSize is null").toBytes();
        checkArgument(standardSplitSizeInBytesLong > 0, "standardSplitSize must be > 0, found: %s", standardSplitSize);
        this.standardSplitSizeInBytes = (double) standardSplitSizeInBytesLong;
    }

    @Override
    public SplitWeight calculateSplitWeight(long splitSizeInBytes)
    {
        double computedWeight = splitSizeInBytes / standardSplitSizeInBytes;
        // Clamp the value between the minimum weight and 1.0 (standard weight)
        return SplitWeight.fromProportion(constrainToRange(computedWeight, minimumWeight, 1.0));
    }
}
