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
package com.facebook.presto.operator.aggregation.fixedhistogram;

import static com.google.common.base.Preconditions.checkArgument;

class FixedHistogramUtils
{
    private FixedHistogramUtils() {}

    public static void validateParameters(int bucketCount, double min, double max)
    {
        checkArgument(bucketCount >= 2, "bucketCount must be at least 2: %s", bucketCount);
        checkArgument(min < max, "min must be smaller than max: %s %s", min, max);
    }

    public static int getIndexForValue(int bucketCount, double min, double max, double value)
    {
        checkArgument(
                value >= min && value < max,
                "value must be within range: %s [%s, %s]", value, min, max);
        return Math.min(
                (int) (bucketCount * (value - min) / (max - min)),
                bucketCount - 1);
    }

    public static double getLeftValueForIndex(int bucketCount, double min, double max, int index)
    {
        return min + index * (max - min) / bucketCount;
    }

    public static double getRightValueForIndex(int bucketCount, double min, double max, int index)
    {
        return Math.min(
                max,
                getLeftValueForIndex(bucketCount, min, max, index + 1));
    }
}
