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
package com.facebook.presto.execution;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class PartitionedSplitsInfo
{
    private static final PartitionedSplitsInfo NO_SPLITS_INFO = new PartitionedSplitsInfo(0, 0);

    private final int count;
    private final long weightSum;

    private PartitionedSplitsInfo(int splitCount, long splitsWeightSum)
    {
        this.count = splitCount;
        this.weightSum = splitsWeightSum;
    }

    public int getCount()
    {
        return count;
    }

    public long getWeightSum()
    {
        return weightSum;
    }

    @Override
    public int hashCode()
    {
        return (count * 31) + Long.hashCode(weightSum);
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof PartitionedSplitsInfo)) {
            return false;
        }
        PartitionedSplitsInfo otherInfo = (PartitionedSplitsInfo) other;
        return this == otherInfo || (this.count == otherInfo.count && this.weightSum == otherInfo.weightSum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("count", count)
                .add("weightSum", weightSum)
                .toString();
    }

    public static PartitionedSplitsInfo forSplitCountAndWeightSum(int splitCount, long weightSum)
    {
        // Avoid allocating for the "no splits" case, also mask potential race condition between
        // count and weight updates that might yield a positive weight with a count of 0
        return splitCount == 0 ? NO_SPLITS_INFO : new PartitionedSplitsInfo(splitCount, weightSum);
    }

    public static PartitionedSplitsInfo forZeroSplits()
    {
        return NO_SPLITS_INFO;
    }
}
