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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraConnectorRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final CassandraSession cassandraSession;

    @Inject
    public CassandraConnectorRecordSinkProvider(CassandraSession cassandraSession)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        CassandraOutputTableHandle handle = (CassandraOutputTableHandle) tableHandle;

        return new CassandraRecordSink(
                cassandraSession,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                true);
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraInsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        CassandraInsertTableHandle handle = (CassandraInsertTableHandle) tableHandle;

        return new CassandraRecordSink(
                cassandraSession,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                false);
    }
}
