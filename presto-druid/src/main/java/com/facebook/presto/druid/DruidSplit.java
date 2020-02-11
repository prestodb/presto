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
package com.facebook.presto.druid;

import com.facebook.presto.druid.metadata.DruidSegmentInfo;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class DruidSplit
        implements ConnectorSplit
{
    private final DruidSegmentInfo segmentInfo;
    private final HostAddress address;

    @JsonCreator
    public DruidSplit(
            @JsonProperty("segmentInfo") DruidSegmentInfo segmentInfo,
            @JsonProperty("address") HostAddress address)
    {
        this.segmentInfo = requireNonNull(segmentInfo, "segment info is null");
        this.address = requireNonNull(address, "address info is null");
    }

    @JsonProperty
    public DruidSegmentInfo getSegmentInfo()
    {
        return segmentInfo;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
