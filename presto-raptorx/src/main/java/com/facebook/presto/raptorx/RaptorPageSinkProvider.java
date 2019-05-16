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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.StorageConfig;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkProperties;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final DataSize maxBufferSize;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, PageSorter pageSorter, StorageConfig config)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.maxBufferSize = config.getMaxBufferSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Raptor connector does not support partition commit");

        RaptorOutputTableHandle handle = (RaptorOutputTableHandle) outputHandle;
        return new RaptorPageSink(
                storageManager,
                pageSorter,
                handle.getTransactionId(),
                handle.getTableId(),
                toColumnIds(handle.getColumnHandles()),
                toColumnTypes(handle.getColumnHandles()),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                handle.getCompressionType(),
                maxBufferSize);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Raptor connector does not support partition commit");

        RaptorInsertTableHandle handle = (RaptorInsertTableHandle) insertHandle;
        return new RaptorPageSink(
                storageManager,
                pageSorter,
                handle.getTransactionId(),
                handle.getTableId(),
                toColumnIds(handle.getColumnHandles()),
                toColumnTypes(handle.getColumnHandles()),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                handle.getCompressionType(),
                maxBufferSize);
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columns)
    {
        return columns.stream()
                .map(RaptorColumnHandle::getColumnId)
                .collect(toImmutableList());
    }

    private static List<Type> toColumnTypes(List<RaptorColumnHandle> columns)
    {
        return columns.stream()
                .map(RaptorColumnHandle::getColumnType)
                .collect(toImmutableList());
    }
}
