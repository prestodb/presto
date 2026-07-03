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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Long> datasetVersion;
    private final OptionalLong limit;

    public LanceTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, Optional.empty(), OptionalLong.empty());
    }

    public LanceTableHandle(String schemaName, String tableName, Optional<Long> datasetVersion)
    {
        this(schemaName, tableName, datasetVersion, OptionalLong.empty());
    }

    @JsonCreator
    public LanceTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("datasetVersion") Optional<Long> datasetVersion,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.datasetVersion = requireNonNull(datasetVersion, "datasetVersion is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Long> getDatasetVersion()
    {
        return datasetVersion;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    public boolean hasLimit()
    {
        return limit.isPresent();
    }

    public LanceTableHandle withLimit(long limit)
    {
        return new LanceTableHandle(schemaName, tableName, datasetVersion, OptionalLong.of(limit));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, datasetVersion, limit);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        LanceTableHandle other = (LanceTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.datasetVersion, other.datasetVersion) &&
                Objects.equals(this.limit, other.limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("datasetVersion", datasetVersion)
                .add("limit", limit)
                .toString();
    }
}
