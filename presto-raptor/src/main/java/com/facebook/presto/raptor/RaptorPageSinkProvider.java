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
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.PageSorter;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final String nodeId;
    private final PageSorter pageSorter;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, CurrentNodeId currentNodeId, PageSorter pageSorter)
    {
        this(storageManager, currentNodeId.toString(), pageSorter);
    }

    public RaptorPageSinkProvider(StorageManager storageManager, String nodeId, PageSorter pageSorter)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.pageSorter = checkNotNull(pageSorter, "pageSorter is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                nodeId,
                pageSorter,
                storageManager,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                optionalColumnId(handle.getSampleWeightColumnHandle()),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorInsertTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(tableHandle, RaptorInsertTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                nodeId,
                pageSorter,
                storageManager,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                Optional.empty(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders());
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
    }

    private static Optional<Long> optionalColumnId(RaptorColumnHandle handle)
    {
        return Optional.ofNullable(handle).map(RaptorColumnHandle::getColumnId);
    }
}
