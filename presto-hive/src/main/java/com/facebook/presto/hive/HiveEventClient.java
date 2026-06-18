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

import com.facebook.airlift.event.client.AbstractEventClient;
import com.facebook.airlift.log.Logger;

public class HiveEventClient
        extends AbstractEventClient
{
    private static final Logger log = Logger.get(HiveEventClient.class);

    @Override
    public <T> void postEvent(T event)
    {
        if (!(event instanceof WriteCompletedEvent)) {
            return;
        }
        WriteCompletedEvent writeCompletedEvent = (WriteCompletedEvent) event;
        log.debug("File created: query: %s, schema: %s, table: %s, partition: '%s', format: %s, size: %s, path: %s",
                writeCompletedEvent.getQueryId(),
                writeCompletedEvent.getSchemaName(),
                writeCompletedEvent.getTableName(),
                writeCompletedEvent.getPartitionName(),
                writeCompletedEvent.getStorageFormat(),
                writeCompletedEvent.getBytes(),
                writeCompletedEvent.getPath());
    }
}
