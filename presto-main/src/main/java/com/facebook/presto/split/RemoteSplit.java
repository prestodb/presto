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

import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RemoteSplit
        implements ConnectorSplit
{
    private final Location location;
    private final TaskId remoteSourceTaskId;

    @JsonCreator
    public RemoteSplit(@JsonProperty("location") Location location, @JsonProperty("remoteSourceTaskId") TaskId remoteSourceTaskId)
    {
        this.location = requireNonNull(location, "location is null");
        this.remoteSourceTaskId = requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");
    }

    @JsonProperty
    public Location getLocation()
    {
        return location;
    }

    @JsonProperty
    public TaskId getRemoteSourceTaskId()
    {
        return remoteSourceTaskId;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .add("remoteSourceTaskId", remoteSourceTaskId)
                .toString();
    }
}
