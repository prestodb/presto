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

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryQueueDefinition
{
    private final String name;
    private final int maxConcurrent;
    private final int maxQueued;
    // new attributes:
    private final DataSize maxMemory;
    private final Duration maxCpuTime;
    private final Duration maxQueryCpuTime;
    private final Duration runtimeCap;
    private final Duration queuedTimeCap;
    private final boolean isPublic;
    public QueryQueueDefinition(String name,
                                int maxConcurrent,
                                int maxQueued,
                                DataSize maxMemory,
                                Duration maxCpuTime,
                                Duration maxQueryCpuTime,
                                Duration runtimeCap,
                                Duration queuedTimeCap,
                                boolean isPublic)
    {
        this.name = requireNonNull(name, "name is null");
        checkArgument(maxConcurrent > 0, "maxConcurrent must be positive");
        checkArgument(maxQueued > 0, "maxQueued must be positive");
        this.maxConcurrent = maxConcurrent;
        this.maxQueued = maxQueued;

        //new attributes:
        this.maxMemory = requireNonNull(maxMemory, "max memory is null");
        this.maxCpuTime = requireNonNull(maxCpuTime, "max cpu time is null");
        this.maxQueryCpuTime = requireNonNull(maxQueryCpuTime, "max query cpu time is null");
        this.runtimeCap = requireNonNull(runtimeCap, "runtime cap is null");
        this.queuedTimeCap = requireNonNull(queuedTimeCap, "queued time cap is null");
        this.isPublic = isPublic;
    }

    String getName()
    {
        return name;
    }

    public int getMaxConcurrent()
    {
        return maxConcurrent;
    }

    public int getMaxQueued()
    {
        return maxQueued;
    }

    public DataSize getMaxMemory()
    {
        return maxMemory;
    }

    public Duration getMaxCpuTime()
    {
        return maxCpuTime;
    }

    public Duration getMaxQueryCpuTime()
    {
        return maxQueryCpuTime;
    }

    public Duration getRuntimeCap()
    {
        return runtimeCap;
    }

    public Duration getQueuedTimeCap()
    {
        return queuedTimeCap;
    }

    public boolean getIsPublic()
    {
        return isPublic;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof QueryQueueDefinition)) {
            return false;
        }

        QueryQueueDefinition that = (QueryQueueDefinition) other;
        return getName() == that.getName();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
