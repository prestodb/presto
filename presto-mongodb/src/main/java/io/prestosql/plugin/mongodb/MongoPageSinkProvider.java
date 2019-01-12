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
package io.prestosql.plugin.mongodb;

import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

public class MongoPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MongoClientConfig config;
    private final MongoSession mongoSession;

    @Inject
    public MongoPageSinkProvider(MongoClientConfig config, MongoSession mongoSession)
    {
        this.config = config;
        this.mongoSession = mongoSession;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        MongoOutputTableHandle handle = (MongoOutputTableHandle) outputTableHandle;
        return new MongoPageSink(config, mongoSession, session, handle.getSchemaTableName(), handle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        MongoInsertTableHandle handle = (MongoInsertTableHandle) insertTableHandle;
        return new MongoPageSink(config, mongoSession, session, handle.getSchemaTableName(), handle.getColumns());
    }
}
