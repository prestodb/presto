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
package io.prestosql.plugin.hive;

import io.airlift.event.client.EventField;
import io.airlift.event.client.EventField.EventFieldMapping;
import io.airlift.event.client.EventType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.time.Instant;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("WriteCompletedEvent")
public class WriteCompletedEvent
{
    private final String queryId;
    private final String path;
    private final String schemaName;
    private final String tableName;
    private final String partitionName;
    private final String storageFormat;
    private final String writerImplementation;
    private final String prestoVersion;
    private final String host;
    private final String principal;
    private final String environment;
    private final Map<String, String> sessionProperties;
    private final Long bytes;
    private final long rows;
    private final Instant timestamp = Instant.now();

    public WriteCompletedEvent(
            String queryId,
            String path,
            String schemaName,
            String tableName,
            @Nullable String partitionName,
            String storageFormat,
            String writerImplementation,
            String prestoVersion,
            String serverAddress,
            @Nullable String principal,
            String environment,
            Map<String, String> sessionProperties,
            @Nullable Long bytes,
            long rows)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.path = requireNonNull(path, "path is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionName = partitionName;
        this.storageFormat = requireNonNull(storageFormat, "storageFormat is null");
        this.writerImplementation = requireNonNull(writerImplementation, "writerImplementation is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.host = requireNonNull(serverAddress, "serverAddress is null");
        this.principal = principal;
        this.environment = requireNonNull(environment, "environment is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.bytes = bytes;
        this.rows = rows;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getPath()
    {
        return path;
    }

    @EventField
    public String getSchemaName()
    {
        return schemaName;
    }

    @EventField
    public String getTableName()
    {
        return tableName;
    }

    @Nullable
    @EventField
    public String getPartitionName()
    {
        return partitionName;
    }

    @EventField
    public String getStorageFormat()
    {
        return storageFormat;
    }

    @EventField
    public String getWriterImplementation()
    {
        return writerImplementation;
    }

    @EventField
    public String getPrestoVersion()
    {
        return prestoVersion;
    }

    @EventField(fieldMapping = EventFieldMapping.HOST)
    public String getHost()
    {
        return host;
    }

    @Nullable
    @EventField
    public String getPrincipal()
    {
        return principal;
    }

    @EventField
    public String getEnvironment()
    {
        return environment;
    }

    @EventField
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    @Nullable
    @EventField
    public Long getBytes()
    {
        return bytes;
    }

    @EventField
    public long getRows()
    {
        return rows;
    }

    @EventField(fieldMapping = EventFieldMapping.TIMESTAMP)
    public Instant getTimestamp()
    {
        return timestamp;
    }
}
