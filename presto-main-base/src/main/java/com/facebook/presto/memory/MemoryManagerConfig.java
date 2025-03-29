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
package com.facebook.presto.memory;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "experimental.cluster-memory-manager-enabled",
        "query.low-memory-killer.enabled"})
public class MemoryManagerConfig
{
    // enforced against user memory allocations
    private DataSize maxQueryMemory = new DataSize(20, GIGABYTE);
    private DataSize softMaxQueryMemory;
    // enforced against user + system memory allocations (default is maxQueryMemory * 2)
    private DataSize maxQueryTotalMemory;
    private DataSize softMaxQueryTotalMemory;
    private String lowMemoryKillerPolicy = LowMemoryKillerPolicy.NONE;
    private Duration killOnOutOfMemoryDelay = new Duration(5, MINUTES);
    private boolean tableFinishOperatorMemoryTrackingEnabled;

    public String getLowMemoryKillerPolicy()
    {
        return lowMemoryKillerPolicy;
    }

    @Config("query.low-memory-killer.policy")
    public MemoryManagerConfig setLowMemoryKillerPolicy(String lowMemoryKillerPolicy)
    {
        this.lowMemoryKillerPolicy = lowMemoryKillerPolicy;
        return this;
    }

    @NotNull
    @MinDuration("5s")
    public Duration getKillOnOutOfMemoryDelay()
    {
        return killOnOutOfMemoryDelay;
    }

    @Config("query.low-memory-killer.delay")
    @ConfigDescription("Delay between cluster running low on memory and invoking killer")
    public MemoryManagerConfig setKillOnOutOfMemoryDelay(Duration killOnOutOfMemoryDelay)
    {
        this.killOnOutOfMemoryDelay = killOnOutOfMemoryDelay;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryMemory()
    {
        return maxQueryMemory;
    }

    @Config("query.max-memory")
    public MemoryManagerConfig setMaxQueryMemory(DataSize maxQueryMemory)
    {
        this.maxQueryMemory = maxQueryMemory;
        return this;
    }

    @NotNull
    public DataSize getSoftMaxQueryMemory()
    {
        if (softMaxQueryMemory == null) {
            return getMaxQueryMemory();
        }
        return softMaxQueryMemory;
    }

    @Config("query.soft-max-memory")
    public MemoryManagerConfig setSoftMaxQueryMemory(DataSize softMaxQueryMemory)
    {
        this.softMaxQueryMemory = softMaxQueryMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryTotalMemory()
    {
        if (maxQueryTotalMemory == null) {
            return succinctBytes(maxQueryMemory.toBytes() * 2);
        }
        return maxQueryTotalMemory;
    }

    @Config("query.max-total-memory")
    public MemoryManagerConfig setMaxQueryTotalMemory(DataSize maxQueryTotalMemory)
    {
        this.maxQueryTotalMemory = maxQueryTotalMemory;
        return this;
    }

    @NotNull
    public DataSize getSoftMaxQueryTotalMemory()
    {
        if (softMaxQueryTotalMemory == null) {
            if (maxQueryTotalMemory != null) {
                return maxQueryTotalMemory;
            }
            return succinctBytes(getSoftMaxQueryMemory().toBytes() * 2);
        }
        return softMaxQueryTotalMemory;
    }

    @Config("query.soft-max-total-memory")
    public MemoryManagerConfig setSoftMaxQueryTotalMemory(DataSize softMaxQueryTotalMemory)
    {
        this.softMaxQueryTotalMemory = softMaxQueryTotalMemory;
        return this;
    }

    public boolean isTableFinishOperatorMemoryTrackingEnabled()
    {
        return tableFinishOperatorMemoryTrackingEnabled;
    }

    @Config("table-finish-operator-memory-tracking-enabled")
    public MemoryManagerConfig setTableFinishOperatorMemoryTrackingEnabled(boolean tableFinishOperatorMemoryTrackingEnabled)
    {
        this.tableFinishOperatorMemoryTrackingEnabled = tableFinishOperatorMemoryTrackingEnabled;
        return this;
    }

    public static class LowMemoryKillerPolicy
    {
        public static final String NONE = "none";
        public static final String TOTAL_RESERVATION = "total-reservation";
        public static final String TOTAL_RESERVATION_ON_BLOCKED_NODES = "total-reservation-on-blocked-nodes";
    }
}
