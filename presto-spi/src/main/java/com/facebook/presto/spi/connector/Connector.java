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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

public interface Connector
{
    ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly);

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     */
    ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle);

    ConnectorSplitManager getSplitManager();

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    default ConnectorPageSourceProvider getPageSourceProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    default ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables page at a time
     */
    default ConnectorPageSinkProvider getPageSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables record at a time
     */
    default ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support indexes
     */
    default ConnectorIndexProvider getIndexProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support partitioned table layouts
     */
    default ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the set of system tables provided by this connector
     */
    default Set<SystemTable> getSystemTables()
    {
        return emptySet();
    }

    /**
     * @return the set of procedures provided by this connector
     */
    default Set<Procedure> getProcedures()
    {
        return emptySet();
    }

    /**
     * @return the system properties for this connector
     */
    default List<PropertyMetadata<?>> getSessionProperties()
    {
        return emptyList();
    }

    /**
     * @return the table properties for this connector
     */
    default List<PropertyMetadata<?>> getTableProperties()
    {
        return emptyList();
    }

    /**
     * @throws UnsupportedOperationException if this connector does not have an access control
     */
    default ConnectorAccessControl getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Commit the transaction. Will be called at most once and will not be called if
     * {@link #rollback(ConnectorTransactionHandle)} is called.
     */
    default void commit(ConnectorTransactionHandle transactionHandle)
    {
    }

    /**
     * Rollback the transaction. Will be called at most once and will not be called if
     * {@link #commit(ConnectorTransactionHandle)} is called.
     * Note: calls to this method may race with calls to the ConnectorMetadata.
     */
    default void rollback(ConnectorTransactionHandle transactionHandle)
    {
    }

    /**
     * True if the connector only supports write statements in independent transactions.
     */
    default boolean isSingleStatementWritesOnly()
    {
        return false;
    }

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    default void shutdown() {}
}
