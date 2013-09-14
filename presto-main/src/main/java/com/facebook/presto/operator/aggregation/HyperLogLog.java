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

import com.google.common.base.Preconditions;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

/**
 * TODO implement 4 bit per bucket optimization
 */
public class HyperLogLog
{
    private final double alpha;
    private final int numberOfBuckets;

    public HyperLogLog(int numberOfBuckets)
    {
        Preconditions.checkArgument(isPowerOf2(numberOfBuckets), "numberOfBuckets must be a power of 2");
        Preconditions.checkArgument(numberOfBuckets > 0, "numberOfBuckets must be > 0");

        this.numberOfBuckets = numberOfBuckets;
        alpha = 1 / (2 * Math.log(2) * (1 + (3 * Math.log(2) - 1) / numberOfBuckets));
    }

    public int getSizeInBytes()
    {
        return numberOfBuckets * SizeOf.SIZE_OF_BYTE;
    }

    public void update(long hash, Slice slice, int offset)
    {
        int bucketMask = numberOfBuckets - 1;
        int bucket = (int) (hash & bucketMask);

        // set the lsb to 1 so that they don't introduce an error if the hash happens to be almost all 0 (very unlikely, but...)
        int highestBit = Long.numberOfLeadingZeros(hash | bucketMask) + 1;

        int previous = slice.getByte(offset + bucket);
        int updated = Math.max(highestBit, previous);

        slice.setByte(offset + bucket, updated); // write new value
    }

    public void mergeInto(Slice destination, int destinationOffset, Slice source, int sourceOffset)
    {
        // TODO: consider doing this long at a time
        for (int bucket = 0; bucket < numberOfBuckets; bucket++) {
            int previous = destination.getByte(destinationOffset + bucket);
            int updated = source.getByte(sourceOffset + bucket);

            if (updated > previous) {
                destination.setByte(destinationOffset + bucket, updated);
            }
        }
    }

    public long estimate(Slice slice, int offset)
    {
        double currentSum = 0;
        int zeroBuckets = 0;

        for (int bucket = 0; bucket < numberOfBuckets; bucket++) {
            int value = slice.getByte(offset + bucket);

            if (value == 0) {
                zeroBuckets++;
            }

            currentSum += 1.0 / (1L << value);
        }

        double result = numberOfBuckets * numberOfBuckets * alpha / currentSum;

        // adjust for small cardinalities
        if (zeroBuckets > 0.03 * numberOfBuckets) {
            result = numberOfBuckets * Math.log(numberOfBuckets * 1.0 / zeroBuckets);
        }

        return Math.round(result);
    }

    public double getStandardError()
    {
        return 1.04 / Math.sqrt(numberOfBuckets);
    }

    private static boolean isPowerOf2(long value)
    {
        return (value & value - 1) == 0;
    }
}
