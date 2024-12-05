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
package com.facebook.presto.sql.planner.planPrinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OperatorInputStats
{
    private final long totalDrivers;
    private final long inputPositions;
    private final double sumSquaredInputPositions;

    @JsonCreator
    public OperatorInputStats(
            @JsonProperty("totalDrivers") long totalDrivers,
            @JsonProperty("inputPositions") long inputPositions,
            @JsonProperty("sumSquaredInputPositions") double sumSquaredInputPositions)
    {
        this.totalDrivers = totalDrivers;
        this.inputPositions = inputPositions;
        this.sumSquaredInputPositions = sumSquaredInputPositions;
    }

    @JsonProperty
    public long getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public long getInputPositions()
    {
        return inputPositions;
    }

    @JsonProperty
    public double getSumSquaredInputPositions()
    {
        return sumSquaredInputPositions;
    }

    public static OperatorInputStats merge(OperatorInputStats first, OperatorInputStats second)
    {
        return new OperatorInputStats(
                first.totalDrivers + second.totalDrivers,
                first.inputPositions + second.inputPositions,
                first.sumSquaredInputPositions + second.sumSquaredInputPositions);
    }
}
