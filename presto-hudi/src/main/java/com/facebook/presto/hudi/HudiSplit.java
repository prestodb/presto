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

package com.facebook.presto.hudi;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.SOFT_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HudiSplit
        implements ConnectorSplit
{
    private final HudiTableHandle table;
    private final String instantTime;
    private final HudiPartition partition;
    private final Optional<HudiFile> baseFile;
    private final List<HudiFile> logFiles;
    private final List<HostAddress> addresses;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("table") HudiTableHandle table,
            @JsonProperty("instantTime") String instantTime,
            @JsonProperty("partition") HudiPartition partition,
            @JsonProperty("baseFile") Optional<HudiFile> baseFile,
            @JsonProperty("logFiles") List<HudiFile> logFiles,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        this.table = requireNonNull(table, "table is null");
        this.instantTime = requireNonNull(instantTime, "instantTime is null");
        this.partition = requireNonNull(partition, "partition is null");
        this.baseFile = requireNonNull(baseFile, "baseFile is null");
        this.logFiles = requireNonNull(logFiles, "logFiles is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.nodeSelectionStrategy = requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @JsonProperty
    public HudiTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public String getInstantTime()
    {
        return instantTime;
    }

    @JsonProperty
    public HudiPartition getPartition()
    {
        return partition;
    }

    @JsonProperty
    public Optional<HudiFile> getBaseFile()
    {
        return baseFile;
    }

    @JsonProperty
    public List<HudiFile> getLogFiles()
    {
        return logFiles;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        if (getNodeSelectionStrategy() == SOFT_AFFINITY) {
            return baseFile.map(file -> nodeProvider.get(file.getPath(), 2)).orElse(addresses);
        }
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public Object getSplitIdentifier()
    {
        return this;
    }

    @Override
    public OptionalLong getSplitSizeInBytes()
    {
        return OptionalLong.empty();
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .toString();
    }
}
