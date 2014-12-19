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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final String nodeId;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, CurrentNodeId currentNodeId)
    {
        this(storageManager, currentNodeId.toString());
    }

    public RaptorPageSinkProvider(StorageManager storageManager, String nodeId)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                nodeId,
                storageManager,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                optionalColumnId(handle.getSampleWeightColumnHandle()));
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorInsertTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(tableHandle, RaptorInsertTableHandle.class, "tableHandle");
        return new RaptorPageSink(
                nodeId,
                storageManager,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                Optional.<Long>absent());
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columnHandles)
    {
        return FluentIterable.from(columnHandles).transform(columnIdGetter()).toList();
    }

    private static Optional<Long> optionalColumnId(RaptorColumnHandle handle)
    {
        return Optional.fromNullable(handle).transform(columnIdGetter());
    }

    private static Function<RaptorColumnHandle, Long> columnIdGetter()
    {
        return RaptorColumnHandle::getColumnId;
    }
}
