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
package com.facebook.presto.raptor.event;

import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

@Immutable
@EventType("ShardOperationEvent")
public class ShardOperationEvent
{
    private final Instant timestamp = Instant.now();

    private final String queryId;
    private final String user;
    private final String source;
    private final String environment;

    private final long tableId;
    private final long columns;
    private final long shardsCreated;
    private final long shardsRemoved;

    ShardOperationEvent(
            String queryId,
            String user,
            String source,
            String environment,
            long tableId,
            long columns,
            long shardsCreated,
            long shardsRemoved)
    {
        this.queryId = queryId;
        this.user = user;
        this.source = source;
        this.environment = requireNonNull(environment, "environment is null");
        this.tableId = tableId;
        this.columns = columns;
        this.shardsCreated = shardsCreated;
        this.shardsRemoved = shardsRemoved;
    }

    @EventField
    public Instant getTimestamp()
    {
        return timestamp;
    }

    @Nullable
    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @Nullable
    @EventField
    public String getUser()
    {
        return user;
    }

    @Nullable
    @EventField
    public String getSource()
    {
        return source;
    }

    @EventField
    public String getEnvironment()
    {
        return environment;
    }

    @EventField
    public long getTableId()
    {
        return tableId;
    }

    @EventField
    public long getColumns()
    {
        return columns;
    }

    @EventField
    public long getShardsCreated()
    {
        return shardsCreated;
    }

    @EventField
    public long getShardsRemoved()
    {
        return shardsRemoved;
    }
}
