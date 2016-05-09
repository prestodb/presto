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
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.google.inject.Inject;

import static com.facebook.presto.mongodb.TypeUtils.checkType;

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
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        MongoOutputTableHandle handle = checkType(outputTableHandle, MongoOutputTableHandle.class, "outputTableHandle");
        return new MongoPageSink(config, mongoSession, session, handle.getSchemaTableName(), handle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        MongoInsertTableHandle handle = checkType(insertTableHandle, MongoInsertTableHandle.class, "insertTableHandle");
        return new MongoPageSink(config, mongoSession, session, handle.getSchemaTableName(), handle.getColumns());
    }
}
