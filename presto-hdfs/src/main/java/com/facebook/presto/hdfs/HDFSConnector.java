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

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
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
//    private final ConnectorNodePartitioningProvider nodePartitioningProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, HDFSMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public HDFSConnector(
            LifeCycleManager lifeCycleManager,
            HDFSMetadataFactory hdfsMetadataFactory,
            HDFSSplitManager hdfsSplitManager,
            HDFSPageSourceProvider hdfsPageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.hdfsMetadataFactory = requireNonNull(hdfsMetadataFactory, "hdfsMetadataFactory is null");
        this.hdfsSplitManager = requireNonNull(hdfsSplitManager, "hdfsSplitManager is null");
        this.hdfsPageSourceProvider = requireNonNull(hdfsPageSourceProvider, "hdfsPageSourceProvider is null");
//        this.nodePartitioningProvider = requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        HDFSTransactionHandle transaction = new HDFSTransactionHandle();
        transactions.putIfAbsent(transaction, hdfsMetadataFactory.create());
        return transaction;
    }

    /**
     * Guaranteed to be called at most once per transaction. The returned metadata will only be accessed
     * in a single threaded context.
     *
     * @param transactionHandle transaction handle
     */
    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        HDFSMetadata metadata = transactions.get(transactionHandle);
        checkArgument(metadata != null, "no such transaction: %s", transactionHandle);
        return hdfsMetadataFactory.create();
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return hdfsSplitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return hdfsPageSourceProvider;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    /**
     * @throws UnsupportedOperationException if this connector does not support partitioned table layouts
     */
//    @Override
//    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
//    {
//        return nodePartitioningProvider;
//    }

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
        try {
            hdfsMetadataFactory.shutdown();
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            logger.error(e, "Error shutting down hdfs connector");
        }
    }
}
