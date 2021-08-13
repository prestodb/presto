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

import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class HiveMetadataUpdateHandle
        implements ConnectorMetadataUpdateHandle
{
    private final UUID requestId;
    private final SchemaTableName schemaTableName;

    // partitionName will be null for un-partitioned tables
    private final Optional<String> partitionName;

    // fileName will be null when this class is used to represent metadata request
    private final Optional<String> fileName;

    @JsonCreator
    public HiveMetadataUpdateHandle(
            @JsonProperty("requestId") UUID requestId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("partitionName") Optional<String> partitionName,
            @JsonProperty("fileName") Optional<String> fileName)
    {
        this.requestId = requireNonNull(requestId, "requestId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.partitionName = requireNonNull(partitionName, "partitionName is null");
        this.fileName = requireNonNull(fileName, "fileName is null");
    }

    @JsonProperty
    public UUID getRequestId()
    {
        return requestId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public Optional<String> getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty("fileName")
    public Optional<String> getMetadataUpdate()
    {
        return fileName;
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
        HiveMetadataUpdateHandle o = (HiveMetadataUpdateHandle) obj;
        return requestId.equals(o.requestId) &&
                schemaTableName.equals(o.schemaTableName) &&
                partitionName.equals(o.partitionName) &&
                fileName.equals(o.fileName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(requestId, schemaTableName, partitionName, fileName);
    }
}
