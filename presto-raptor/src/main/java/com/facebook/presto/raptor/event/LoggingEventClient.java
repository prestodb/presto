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

import io.airlift.event.client.AbstractEventClient;
import io.airlift.log.Logger;

import java.io.IOException;

public class LoggingEventClient
        extends AbstractEventClient
{
    private static final Logger log = Logger.get(LoggingEventClient.class);

    @Override
    public <T> void postEvent(T event)
            throws IOException
    {
        if (!(event instanceof ShardOperationEvent)) {
            return;
        }

        ShardOperationEvent shardOperationEvent = (ShardOperationEvent) event;
        log.info("Shard operation finished (query %s, user: %s, source: %s, environment: %s, tableId: %s, columns: %s, shardsCreated: %s, shardsRemoved: %s)",
                shardOperationEvent.getQueryId(),
                shardOperationEvent.getUser(),
                shardOperationEvent.getSource(),
                shardOperationEvent.getEnvironment(),
                shardOperationEvent.getTableId(),
                shardOperationEvent.getColumns(),
                shardOperationEvent.getShardsCreated(),
                shardOperationEvent.getShardsRemoved());
    }
}
