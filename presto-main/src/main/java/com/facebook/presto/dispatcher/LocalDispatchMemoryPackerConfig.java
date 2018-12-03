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
package com.facebook.presto.dispatcher;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class LocalDispatchMemoryPackerConfig
{
    // TODO: Add test for config

    // TODO: Remove unnecessary knobs when this feature is mature
    private double deadlineRatio = 0.1;
    private double totalToUserMemoryRatio = 1;
    private double overcommitRatio = 0;

    private Duration queryWarmupTime = new Duration(30, SECONDS);
    private Duration averageQueryExecutionTime = new Duration(30, SECONDS);
    private DataSize averageQueryPeakMemory = new DataSize(1, GIGABYTE);
    private DataSize minimumSystemMemory = new DataSize(5, GIGABYTE);

    public double getDeadlineRatio()
    {
        return deadlineRatio;
    }

    @Config("local-memory-packer.deadline-ratio")
    @ConfigDescription("Queries are expected to spend no longer than (deadline-ratio * estimated execution time) waiting for memory")
    public LocalDispatchMemoryPackerConfig setDeadlineRatio(double deadlineRatio)
    {
        this.deadlineRatio = deadlineRatio;
        return this;
    }

    @Min(1)
    public double getTotalToUserMemoryRatio()
    {
        return totalToUserMemoryRatio;
    }

    @Config("local-memory-packer.total-to-user-memory-ratio")
    @ConfigDescription("The expected ratio of total to user memory")
    public LocalDispatchMemoryPackerConfig setTotalToUserMemoryRatio(double totalToUserMemoryRatio)
    {
        this.totalToUserMemoryRatio = totalToUserMemoryRatio;
        return this;
    }

    @Min(0)
    public double getOvercommitRatio()
    {
        return overcommitRatio;
    }

    @Config("local-memory-packer.overcommit-ratio")
    @ConfigDescription("The fraction by which to over-commit the memory available in the cluster")
    public LocalDispatchMemoryPackerConfig setOvercommitRatio(double overcommitRatio)
    {
        this.overcommitRatio = overcommitRatio;
        return this;
    }

    @NotNull
    public Duration getQueryWarmupTime()
    {
        return queryWarmupTime;
    }

    @Config("local-memory-packer.warmup-time")
    @ConfigDescription("The expected time taken for a query to be scheduled in the cluster before it begins to use non-trivial amounts of memory")
    public LocalDispatchMemoryPackerConfig setQueryWarmupTime(Duration queryWarmupTime)
    {
        this.queryWarmupTime = queryWarmupTime;
        return this;
    }

    @NotNull
    public Duration getAverageQueryExecutionTime()
    {
        return averageQueryExecutionTime;
    }

    @Config("local-memory-packer.average-execution-time")
    @ConfigDescription("The average execution time for a query")
    public LocalDispatchMemoryPackerConfig setAverageQueryExecutionTime(Duration averageQueryExecutionTime)
    {
        this.averageQueryExecutionTime = averageQueryExecutionTime;
        return this;
    }

    @NotNull
    public DataSize getAverageQueryPeakMemory()
    {
        return averageQueryPeakMemory;
    }

    @Config("local-memory-packer.average-peak-memory")
    @ConfigDescription("The average peak memory for a query")
    public LocalDispatchMemoryPackerConfig setAverageQueryPeakMemory(DataSize averageQueryPeakMemory)
    {
        this.averageQueryPeakMemory = averageQueryPeakMemory;
        return this;
    }

    @NotNull
    public DataSize getMinimumSystemMemory()
    {
        return minimumSystemMemory;
    }

    @Config("local-memory-packer.minimum-system-memory")
    @ConfigDescription("The minimum amount of system memory that a non-trivial query is expected to consume")
    public LocalDispatchMemoryPackerConfig setMinimumSystemMemory(DataSize minimumSystemMemory)
    {
        this.minimumSystemMemory = minimumSystemMemory;
        return this;
    }
}
