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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ThriftTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<List<String>> bucketedBy;
    private final int bucketCount;

    @JsonCreator
    public ThriftTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("bucketedBy") Optional<List<String>> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.bucketedBy = requireNonNull(bucketedBy, "bucketedBy is null");
        this.bucketCount = bucketCount;
    }

    public ThriftTableHandle(SchemaTableName schemaTableName, Optional<List<String>> bucketedBy, int bucketCount)
    {
        this(schemaTableName.getSchemaName(), schemaTableName.getTableName(), bucketedBy, bucketCount);
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
    public Optional<List<String>> getBucketedBy()
    {
        return bucketedBy;
    }

    public int getBucketCount()
    {
        return bucketCount;
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
        ThriftTableHandle other = (ThriftTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, bucketedBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", getSchemaName())
                .add("tableName", getTableName())
                .add("bucketedBy", getBucketedBy())
                .toString();
    }
}
