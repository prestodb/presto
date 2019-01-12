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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MongoPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MongoSession mongoSession;

    @Inject
    public MongoPageSourceProvider(MongoSession mongoSession)
    {
        this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        MongoSplit mongodbSplit = (MongoSplit) split;

        ImmutableList.Builder<MongoColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : requireNonNull(columns, "columns is null")) {
            handles.add((MongoColumnHandle) handle);
        }

        return new MongoPageSource(mongoSession, mongodbSplit, handles.build());
    }
}
