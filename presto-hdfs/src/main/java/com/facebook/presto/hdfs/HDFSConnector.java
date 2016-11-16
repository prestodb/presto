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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorIndexProvider;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSConnector
implements Connector
{
    private final Logger logger = Logger.get(HDFSConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final HDFSMetadataFactory hdfsMetadataFactory;
    private final HDFSSplitManager hdfsSplitManager;
    private final HDFSPageSourceProvider hdfsPageSourceProvider;
    private final HDFSPageSinkProvider hdfsPageSinkProvider;

    @Inject
    public HDFSConnector(
            LifeCycleManager lifeCycleManager,
            HDFSMetadataFactory hdfsMetadataFactory,
            HDFSSplitManager hdfsSplitManager,
            HDFSPageSourceProvider hdfsPageSourceProvider,
            HDFSPageSinkProvider hdfsPageSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.hdfsMetadataFactory = requireNonNull(hdfsMetadataFactory, "hdfsMetadataFactory is null");
        this.hdfsSplitManager = requireNonNull(hdfsSplitManager, "hdfsSplitManager is null");
        this.hdfsPageSourceProvider = requireNonNull(hdfsPageSourceProvider, "hdfsPageSourceProvider is null");
        this.hdfsPageSinkProvider = requireNonNull(hdfsPageSinkProvider, "hdfsPageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return null;
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     *
     * @param transactionHandle
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return null;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables page at a time
     */
    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support reading tables record at a time
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables page at a time
     */
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support writing tables record at a time
     */
    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support indexes
     */
    @Override
    public ConnectorIndexProvider getIndexProvider()
    {
        return null;
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support partitioned table layouts
     */
    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return null;
    }

    /**
     * @return the set of system tables provided by this connector
     */
    @Override
    public Set<SystemTable> getSystemTables()
    {
        return null;
    }

    /**
     * @return the schema properties for this connector
     */
    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return null;
    }

    /**
     * @return the table properties for this connector
     */
    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return null;
    }

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    @Override
    public void shutdown()
    {
    }
}
