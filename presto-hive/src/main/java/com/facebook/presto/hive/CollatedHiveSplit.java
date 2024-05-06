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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class CollatedHiveSplit
        implements ConnectorSplit
{
    private final List<HiveSplit> hiveSplits;

    @JsonCreator
    public CollatedHiveSplit(
            @JsonProperty("hiveSplits") List<HiveSplit> hiveSplits)
    {
        this.hiveSplits = requireNonNull(hiveSplits, "hiveSplits is null");
    }

    @JsonProperty
    public List<HiveSplit> getHiveSplits()
    {
        return hiveSplits;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return hiveSplits.isEmpty() ? null : hiveSplits.get(0).getNodeSelectionStrategy();
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return hiveSplits.isEmpty() ? null : hiveSplits.get(0).getPreferredNodes(nodeProvider);
    }

    @Override
    public Object getInfo()
    {
        return null;
    }

    @Override
    public Map<String, String> getInfoMap()
    {
        return null;
    }

    @Override
    public Object getSplitIdentifier()
    {
        return this;
    }

    @Override
    public OptionalLong getSplitSizeInBytes()
    {
        long totalSize = 0;
        for (HiveSplit split : hiveSplits) {
            totalSize += split.getSplitSizeInBytes().orElse(0);
        }
        return OptionalLong.of(totalSize);
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return SplitWeight.standard();
    }
}
