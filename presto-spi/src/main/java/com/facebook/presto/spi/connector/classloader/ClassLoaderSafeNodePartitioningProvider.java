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
package com.facebook.presto.spi.connector.classloader;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

import static java.util.Objects.requireNonNull;

public final class ClassLoaderSafeNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final ConnectorNodePartitioningProvider delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeNodePartitioningProvider(ConnectorNodePartitioningProvider delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBucketFunction(transactionHandle, session, partitioningHandle, partitionChannelTypes, bucketCount);
        }
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.listPartitionHandles(transactionHandle, session, partitioningHandle);
        }
    }

    @Override
    public Optional<ConnectorBucketNodeMap> getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Node> sortedNodes)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBucketNodeMap(transactionHandle, session, partitioningHandle, sortedNodes);
        }
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSplitBucketFunction(transactionHandle, session, partitioningHandle);
        }
    }

    @Override
    public int getBucketCount(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.getBucketCount(transactionHandle, session, partitioningHandle);
        }
    }
}
