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
package com.facebook.presto.orc.metadata.statistics.hll;

import com.google.common.base.Preconditions;

final class Utils
{
    private Utils()
    {
    }

    public static double alpha(int indexBitLength)
    {
        switch (indexBitLength) {
            case 4:
                return 0.673;
            case 5:
                return 0.697;
            case 6:
                return 0.709;
            default:
                return (0.7213 / (1 + 1.079 / numberOfBuckets(indexBitLength)));
        }
    }

    public static boolean isPowerOf2(long value)
    {
        Preconditions.checkArgument(value > 0, "value must be positive");
        return (value & (value - 1)) == 0;
    }

    public static int indexBitLength(int numberOfBuckets)
    {
        Preconditions.checkArgument(isPowerOf2(numberOfBuckets), "numberOfBuckets must be a power of 2, actual: %s", numberOfBuckets);
        return (int) (Math.log(numberOfBuckets) / Math.log(2));
    }

    public static int numberOfBuckets(int indexBitLength)
    {
        return 1 << indexBitLength;
    }

    public static int computeIndex(long hash, int indexBitLength)
    {
        return (int) (hash >>> (Long.SIZE - indexBitLength));
    }

    public static int numberOfLeadingZeros(long hash, int indexBitLength)
    {
        long value = (hash << indexBitLength) | (1L << (indexBitLength - 1)); // place a 1 in the LSB to preserve the original number of leading zeros if the hash happens to be 0
        return Long.numberOfLeadingZeros(value);
    }

    public static int computeValue(long hash, int indexBitLength)
    {
        return numberOfLeadingZeros(hash, indexBitLength) + 1;
    }

    public static double linearCounting(int zeroBuckets, int totalBuckets)
    {
        return totalBuckets * Math.log(totalBuckets * 1.0 / zeroBuckets);
    }
}
