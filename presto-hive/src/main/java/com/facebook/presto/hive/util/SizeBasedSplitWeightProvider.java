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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveSplitWeightProvider;
import com.facebook.presto.spi.SplitWeight;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SizeBasedSplitWeightProvider
        implements HiveSplitWeightProvider
{
    private final double minimumWeight;
    // The configured size for being used to break files into splits,
    // this size corresponds to a "standard" weight proportion of 1.0
    private final double targetSplitSizeInBytes;

    public SizeBasedSplitWeightProvider(double minimumWeight, DataSize targetSplitSize)
    {
        checkArgument(Double.isFinite(minimumWeight) && minimumWeight > 0 && minimumWeight <= 1, "minimumWeight must be > 0 and <= 1, found: %s", minimumWeight);
        this.minimumWeight = minimumWeight;
        long targetSizeInBytes = requireNonNull(targetSplitSize, "targetSplitSize is null").toBytes();
        checkArgument(targetSizeInBytes > 0, "targetSplitSize must be > 0, found: %s", targetSplitSize);
        this.targetSplitSizeInBytes = (double) targetSizeInBytes;
    }

    @Override
    public SplitWeight weightForSplitSizeInBytes(long splitSizeInBytes)
    {
        // Clamp the value be between the minimum weight and 1.0 (standard weight)
        return SplitWeight.fromProportion(Math.min(Math.max(splitSizeInBytes / targetSplitSizeInBytes, minimumWeight), 1.0));
    }
}
