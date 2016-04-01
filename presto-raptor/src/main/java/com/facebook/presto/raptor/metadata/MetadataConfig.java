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
package com.facebook.presto.raptor.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class MetadataConfig
{
    private Duration startupGracePeriod = new Duration(5, MINUTES);
    private Duration reassignmentDelay = new Duration(0, MINUTES);
    private Duration reassignmentInterval = new Duration(0, MINUTES);

    @NotNull
    public Duration getStartupGracePeriod()
    {
        return startupGracePeriod;
    }

    @Config("raptor.startup-grace-period")
    @ConfigDescription("Minimum uptime before allowing bucket or shard reassignments")
    public MetadataConfig setStartupGracePeriod(Duration startupGracePeriod)
    {
        this.startupGracePeriod = startupGracePeriod;
        return this;
    }

    @NotNull
    public Duration getReassignmentDelay()
    {
        return reassignmentDelay;
    }

    @Config("raptor.reassignment-delay")
    @ConfigDescription("Minimum delay before allowing reassignments for a node")
    public MetadataConfig setReassignmentDelay(Duration reassignmentDelay)
    {
        this.reassignmentDelay = reassignmentDelay;
        return this;
    }

    @NotNull
    public Duration getReassignmentInterval()
    {
        return reassignmentInterval;
    }

    @Config("raptor.reassignment-interval")
    @ConfigDescription("Minimum interval between reassignments for different nodes")
    public MetadataConfig setReassignmentInterval(Duration reassignmentInterval)
    {
        this.reassignmentInterval = reassignmentInterval;
        return this;
    }
}
