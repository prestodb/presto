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

import com.facebook.presto.spi.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ShardOperationEventFactory
{
    private final String environment;

    public ShardOperationEventFactory(String environment)
    {
        this.environment = requireNonNull(environment, "environment is null");
    }

    public ShardOperationEvent getShardOperationEvent(
            long tableId,
            long columns,
            long shardsCreated,
            long shardsRemoved,
            Optional<ConnectorSession> session)
    {
        return session.map(connectorSession -> new ShardOperationEvent(
                connectorSession.getQueryId(),
                connectorSession.getUser(),
                connectorSession.getSource().orElse(null),
                environment,
                tableId,
                columns,
                shardsCreated,
                shardsRemoved)).orElseGet(() -> new ShardOperationEvent(null, null, null, environment, tableId, columns, shardsCreated, shardsRemoved));
    }
}
