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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * ODPS specific {@link ConnectorTableHandle}.
 */
public final class MaxComputeTableHandle
        implements ConnectorTableHandle
{
    /**
     * connector id
     */
    private final String connectorId;

    /**
     * The table name.
     */
    private final String tableName;

    /**
     * The project name.
     */
    private final String projectName;

    private final List<MaxComputeColumnHandle> orderedColumnHandles;
    private final Date lastDataModifiedTime;

    @JsonCreator
    public MaxComputeTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("projectName") String projectName,
            @JsonProperty("orderedColumnHandles") List<MaxComputeColumnHandle> orderedColumnHandles,
            @JsonProperty("lastDataModifiedTime") Date lastDataModifiedTime)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.lastDataModifiedTime = requireNonNull(lastDataModifiedTime, "lastDataModifiedTime is null");
        this.projectName = projectName;
        this.orderedColumnHandles = orderedColumnHandles;
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
    public String getProjectName()
    {
        return projectName;
    }

    @JsonProperty
    public List<MaxComputeColumnHandle> getOrderedColumnHandles()
    {
        return orderedColumnHandles;
    }

    @JsonProperty
    public Date getLastDataModifiedTime()
    {
        return lastDataModifiedTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, projectName, tableName, orderedColumnHandles);
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

        MaxComputeTableHandle other = (MaxComputeTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.projectName, other.projectName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.orderedColumnHandles, other.orderedColumnHandles);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("projectName", projectName)
                .add("tableName", tableName)
                .add("orderedColumnHandles", orderedColumnHandles)
                .toString();
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(projectName, tableName);
    }
}
