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
package com.facebook.presto.spi;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Load Statistics for a worker node in the Presto cluster along with the state.
 */
@ThriftStruct
public class NodeLoadMetrics
{
    private final double cpuUsedPercent;
    private final double memoryUsedInBytes;
    private final int numQueuedDrivers;
    private final boolean cpuOverload;
    private final boolean memoryOverload;

    @JsonCreator
    @ThriftConstructor
    public NodeLoadMetrics(
            @JsonProperty("cpuUsedPercent") double cpuUsedPercent,
            @JsonProperty("memoryUsedInBytes") double memoryUsedInBytes,
            @JsonProperty("numQueuedDrivers") int numQueuedDrivers,
            @JsonProperty("cpuOverload") boolean cpuOverload,
            @JsonProperty("memoryOverload") boolean memoryOverload)
    {
        this.cpuUsedPercent = cpuUsedPercent;
        this.memoryUsedInBytes = memoryUsedInBytes;
        this.numQueuedDrivers = numQueuedDrivers;
        this.cpuOverload = cpuOverload;
        this.memoryOverload = memoryOverload;
    }

    @JsonProperty
    @ThriftField(1)
    public double getCpuUsedPercent()
    {
        return cpuUsedPercent;
    }

    @JsonProperty
    @ThriftField(2)
    public double getMemoryUsedInBytes()
    {
        return memoryUsedInBytes;
    }

    @JsonProperty
    @ThriftField(3)
    public int getNumQueuedDrivers()
    {
        return numQueuedDrivers;
    }

    @JsonProperty
    @ThriftField(4)
    public boolean getCpuOverload()
    {
        return cpuOverload;
    }

    @JsonProperty
    @ThriftField(5)
    public boolean getMemoryOverload()
    {
        return memoryOverload;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeLoadMetrics that = (NodeLoadMetrics) o;
        return Double.compare(cpuUsedPercent, that.cpuUsedPercent) == 0 &&
                Double.compare(memoryUsedInBytes, that.memoryUsedInBytes) == 0 &&
                numQueuedDrivers == that.numQueuedDrivers &&
                cpuOverload == that.cpuOverload &&
                memoryOverload == that.memoryOverload;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuUsedPercent, memoryUsedInBytes, numQueuedDrivers, cpuOverload, memoryOverload);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("NodeLoadMetrics{");
        sb.append("cpuUsedPercent=").append(cpuUsedPercent);
        sb.append(", memoryUsedInBytes=").append(memoryUsedInBytes);
        sb.append(", numQueuedDrivers=").append(numQueuedDrivers);
        sb.append(", cpuOverload=").append(cpuOverload);
        sb.append(", memoryOverload=").append(memoryOverload);
        sb.append('}');
        return sb.toString();
    }
}
