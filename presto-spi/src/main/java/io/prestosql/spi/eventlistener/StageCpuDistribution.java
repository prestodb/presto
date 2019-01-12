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
package io.prestosql.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StageCpuDistribution
{
    private final int stageId;
    private final int tasks;
    private final long p25;
    private final long p50;
    private final long p75;
    private final long p90;
    private final long p95;
    private final long p99;
    private final long min;
    private final long max;
    private final long total;
    private final double average;

    @JsonCreator
    public StageCpuDistribution(
            @JsonProperty("stageId") int stageId,
            @JsonProperty("tasks") int tasks,
            @JsonProperty("p25") long p25,
            @JsonProperty("p50") long p50,
            @JsonProperty("p75") long p75,
            @JsonProperty("p90") long p90,
            @JsonProperty("p95") long p95,
            @JsonProperty("p99") long p99,
            @JsonProperty("min") long min,
            @JsonProperty("max") long max,
            @JsonProperty("total") long total,
            @JsonProperty("average") double average)
    {
        this.stageId = stageId;
        this.tasks = tasks;
        this.p25 = p25;
        this.p50 = p50;
        this.p75 = p75;
        this.p90 = p90;
        this.p95 = p95;
        this.p99 = p99;
        this.min = min;
        this.max = max;
        this.total = total;
        this.average = average;
    }

    @JsonProperty
    public int getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public int getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public long getP25()
    {
        return p25;
    }

    @JsonProperty
    public long getP50()
    {
        return p50;
    }

    @JsonProperty
    public long getP75()
    {
        return p75;
    }

    @JsonProperty
    public long getP90()
    {
        return p90;
    }

    @JsonProperty
    public long getP95()
    {
        return p95;
    }

    @JsonProperty
    public long getP99()
    {
        return p99;
    }

    @JsonProperty
    public long getMin()
    {
        return min;
    }

    @JsonProperty
    public long getMax()
    {
        return max;
    }

    @JsonProperty
    public long getTotal()
    {
        return total;
    }

    @JsonProperty
    public double getAverage()
    {
        return average;
    }
}
