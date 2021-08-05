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

import com.datastax.driver.core.ProtocolVersion;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final CassandraSession cassandraSession;
    private final ProtocolVersion protocolVersion;

    @Inject
    public CassandraPageSinkProvider(CassandraSession cassandraSession, CassandraClientConfig cassandraClientConfig)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.protocolVersion = requireNonNull(cassandraClientConfig, "cassandraClientConfig is null").getProtocolVersion();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Cassandra connector does not support page sink commit");
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        CassandraOutputTableHandle handle = (CassandraOutputTableHandle) tableHandle;

        return new CassandraPageSink(
                cassandraSession,
                protocolVersion,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                true);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkContext pageSinkContext)
    {
        checkArgument(!pageSinkContext.isCommitRequired(), "Cassandra connector does not support page sink commit");
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof CassandraInsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        CassandraInsertTableHandle handle = (CassandraInsertTableHandle) tableHandle;

        return new CassandraPageSink(
                cassandraSession,
                protocolVersion,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                false);
    }
}
