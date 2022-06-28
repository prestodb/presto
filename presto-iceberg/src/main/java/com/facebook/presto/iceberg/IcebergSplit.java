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
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileFormat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.SOFT_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IcebergSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final FileFormat fileFormat;
    private final List<HostAddress> addresses;
    private final Map<Integer, String> partitionKeys;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final SplitWeight splitWeight;

    @JsonCreator
    public IcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionKeys") Map<Integer, String> partitionKeys,
            @JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("splitWeight") SplitWeight splitWeight)
    {
        requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.partitionKeys = Collections.unmodifiableMap(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<Integer, String> getPartitionKeys()
    {
        return partitionKeys;
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
            return nodeProvider.get(path, 2);
        }
        return addresses;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("nodeSelectionStrategy", nodeSelectionStrategy)
                .put("splitWeight", splitWeight)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(nodeSelectionStrategy)
                .addValue(splitWeight)
                .toString();
    }
}
