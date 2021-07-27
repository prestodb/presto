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
package com.facebook.presto.maxcompute;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a ODPS specific {@link ConnectorSplit}.
 */
public final class MaxComputeSplit
        implements ConnectorSplit
{
    private final String id;
    private final String connectorId;
    private final String tableName;
    private final String projectName;
    private final String partitionSpecStr;

    private final long start;
    private final long count;
    private final Date lastDataModifiedTime;

    @JsonCreator
    public MaxComputeSplit(
            @JsonProperty("id") String id,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("projectName") String projectName,
            @JsonProperty("partitionSpecStr") String partitionSpecStr,
            @JsonProperty("start") long start,
            @JsonProperty("count") long count,
            @JsonProperty("lastDataModifiedTime") Date lastDataModifiedTime)
    {
        this.id = requireNonNull(id, "id is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.projectName = projectName;
        this.partitionSpecStr = partitionSpecStr;
        this.start = start;
        this.count = count;
        this.lastDataModifiedTime = requireNonNull(lastDataModifiedTime, "lastDataModifiedTime is null");
    }

    @JsonProperty
    public String getId()
    {
        return id;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public String getPartitionSpecStr()
    {
        return partitionSpecStr;
    }

    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public String getProjectName()
    {
        return projectName;
    }

    @JsonProperty
    public Date getLastDataModifiedTime()
    {
        return lastDataModifiedTime;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        if (sortedCandidates == null || sortedCandidates.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "sortedCandidates is null or empty for OdpsSplit");
        }

        return ImmutableList.of();
    }

    @JsonProperty
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(projectName, tableName, partitionSpecStr, lastDataModifiedTime, start, count);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaxComputeSplit other = (MaxComputeSplit) obj;
        return Objects.equals(this.projectName, other.projectName) &&
                Objects.equals(this.partitionSpecStr, other.partitionSpecStr) &&
                Objects.equals(this.lastDataModifiedTime, other.lastDataModifiedTime) &&
                Objects.equals(this.start, other.start) &&
                Objects.equals(this.count, other.count);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("connectorId", connectorId)
                .add("tableName", tableName)
                .add("partitionSpecStr", partitionSpecStr)
                .add("start", start)
                .add("count", count)
                .toString();
    }
}
