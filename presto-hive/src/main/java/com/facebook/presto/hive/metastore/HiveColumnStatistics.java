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

package com.facebook.presto.hive.metastore;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class HiveColumnStatistics
{
    private final Optional<?> lowValue;
    private final Optional<?> highValue;
    private final OptionalLong maxColumnLength;
    private final OptionalDouble averageColumnLength;
    private final OptionalLong trueCount;
    private final OptionalLong falseCount;
    private final OptionalLong nullsCount;
    private final OptionalLong distinctValuesCount;

    public HiveColumnStatistics(
            Optional<?> lowValue,
            Optional<?> highValue,
            OptionalLong maxColumnLength,
            OptionalDouble averageColumnLength,
            OptionalLong trueCount,
            OptionalLong falseCount,
            OptionalLong nullsCount,
            OptionalLong distinctValuesCount)
    {
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.maxColumnLength = maxColumnLength;
        this.averageColumnLength = averageColumnLength;
        this.trueCount = trueCount;
        this.falseCount = falseCount;
        this.nullsCount = nullsCount;
        this.distinctValuesCount = distinctValuesCount;
    }

    public Optional<?> getLowValue()
    {
        return lowValue;
    }

    public Optional<?> getHighValue()
    {
        return highValue;
    }

    public OptionalLong getMaxColumnLength()
    {
        return maxColumnLength;
    }

    public OptionalDouble getAverageColumnLength()
    {
        return averageColumnLength;
    }

    public OptionalLong getTrueCount()
    {
        return trueCount;
    }

    public OptionalLong getFalseCount()
    {
        return falseCount;
    }

    public OptionalLong getNullsCount()
    {
        return nullsCount;
    }

    public OptionalLong getDistinctValuesCount()
    {
        return distinctValuesCount;
    }
}
