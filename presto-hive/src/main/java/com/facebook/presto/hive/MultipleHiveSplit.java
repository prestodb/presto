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
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MultipleHiveSplit
        implements ConnectorSplit
{
    private final List<HiveSplit> hiveSplits;

    @JsonCreator
    public MultipleHiveSplit(@JsonProperty("hiveSplits") List<HiveSplit> hiveSplits)
    {
        requireNonNull(hiveSplits, "hiveSplits is null");
        this.hiveSplits = ImmutableList.copyOf(hiveSplits);
    }

    @JsonProperty
    public List<HiveSplit> getHiveSplits()
    {
        return hiveSplits;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        if (hiveSplits.size() == 1) {
            return hiveSplits.get(0).getNodeSelectionStrategy();
        }
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        if (hiveSplits.size() == 1) {
            return hiveSplits.get(0).getPreferredNodes(sortedCandidates);
        }
        return sortedCandidates;
    }

    @Override
    public Object getInfo()
    {
        if (hiveSplits.size() == 1) {
            return hiveSplits.get(0).getInfo();
        }
        return hiveSplits.stream().map(HiveSplit::getInfo).collect(Collectors.toList());
    }

    @Override
    public OptionalLong getSplitSizeInBytes()
    {
        return OptionalLong.of(
                hiveSplits.stream()
                        .map(HiveSplit::getSplitSizeInBytes)
                        .filter(OptionalLong::isPresent)
                        .mapToLong(OptionalLong::getAsLong)
                        .sum());
    }

    @Override
    public Object getSplitIdentifier()
    {
        if (hiveSplits.size() == 1) {
            return hiveSplits.get(0).getSplitIdentifier();
        }
        return ImmutableMap.builder()
                .put("path", hiveSplits.stream().map(HiveSplit::getPath).collect(Collectors.toList()))
                .put("start", hiveSplits.stream().map(HiveSplit::getStart).collect(Collectors.toList()))
                .put("length", hiveSplits.stream().map(HiveSplit::getLength).collect(Collectors.toList()))
                .build();
    }

    public OptionalInt getReadBucketNumber()
    {
        checkArgument(hiveSplits.size() == 1);
        return hiveSplits.get(0).getReadBucketNumber();
    }

    public CacheQuotaRequirement getCacheQuotaRequirement()
    {
        checkArgument(hiveSplits.size() == 1);
        return hiveSplits.get(0).getCacheQuotaRequirement();
    }

    public long getLength()
    {
        return hiveSplits.stream().mapToLong(HiveSplit::getLength).sum();
    }

    public int getPartitionDataColumnCount()
    {
        checkArgument(hiveSplits.size() == 1);
        return hiveSplits.get(0).getPartitionDataColumnCount();
    }

    public String getPath()
    {
        checkArgument(hiveSplits.size() == 1);
        return hiveSplits.get(0).getPath();
    }
}
