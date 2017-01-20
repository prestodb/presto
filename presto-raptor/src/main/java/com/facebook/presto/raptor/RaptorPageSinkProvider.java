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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.raptor.util.Types.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final DataSize maxBufferSize;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, PageSorter pageSorter, StorageManagerConfig config)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.maxBufferSize = config.getMaxBufferSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                pageSorter,
                storageManager,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                maxBufferSize);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(tableHandle, RaptorInsertTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                pageSorter,
                storageManager,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                maxBufferSize);
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
    }
}
