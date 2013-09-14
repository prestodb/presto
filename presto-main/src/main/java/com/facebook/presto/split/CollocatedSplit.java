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
package com.facebook.presto.split;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class CollocatedSplit
        implements Split
{
    private final Map<PlanNodeId, Split> splits;
    private final List<HostAddress> addresses;
    private final boolean remotelyAccessible;

    @JsonCreator
    public CollocatedSplit(@JsonProperty("splits") Map<PlanNodeId, Split> splits,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("remotelyAccessible") boolean remotelyAccessible)
    {
        this.splits = splits;
        this.addresses = addresses;
        this.remotelyAccessible = remotelyAccessible;
    }

    @JsonProperty
    public Map<PlanNodeId, Split> getSplits()
    {
        return splits;
    }

    @JsonProperty
    @Override
    public boolean isRemotelyAccessible()
    {
        return remotelyAccessible;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return null;
    }
}
