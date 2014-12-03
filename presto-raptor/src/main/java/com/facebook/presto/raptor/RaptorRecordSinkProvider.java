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
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.raptor.util.CurrentNodeId;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class RaptorRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final StorageManager storageManager;
    private final StorageService storageService;
    private final String nodeId;
    private final StorageManagerConfig storageConfig;

    @Inject
    public RaptorRecordSinkProvider(StorageManager storageManager, StorageManagerConfig storageConfig, StorageService storageService, CurrentNodeId currentNodeId)
    {
        this(storageManager, storageConfig, storageService, currentNodeId.toString());
    }

    public RaptorRecordSinkProvider(StorageManager storageManager, StorageManagerConfig storageConfig, StorageService storageService, String nodeId)
    {
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.storageConfig = checkNotNull(storageConfig, "storageConfig is null");
        this.storageService = checkNotNull(storageService, "storageService is null");
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        RaptorOutputTableHandle handle = checkType(tableHandle, RaptorOutputTableHandle.class, "tableHandle");
        return new RaptorRecordSink(
                nodeId,
                storageManager,
                storageService,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                optionalColumnId(handle.getSampleWeightColumnHandle()),
                Optional.fromNullable(storageConfig.getRowsPerShard()),
                Optional.fromNullable(storageConfig.getBucketCount()),
                ImmutableList.<Long>of());
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        RaptorInsertTableHandle handle = checkType(tableHandle, RaptorInsertTableHandle.class, "tableHandle");
        return new RaptorRecordSink(
                nodeId,
                storageManager,
                storageService,
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                Optional.<Long>absent(),
                Optional.fromNullable(storageConfig.getRowsPerShard()),
                Optional.fromNullable(storageConfig.getBucketCount()),
                toColumnIds(getBucketKeyColumnHandles(handle.getColumnHandles())));
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
        return new Function<RaptorColumnHandle, Long>()
        {
            @Override
            public Long apply(RaptorColumnHandle handle)
            {
                return handle.getColumnId();
            }
        };
    }

    private static List<RaptorColumnHandle> getBucketKeyColumnHandles(List<RaptorColumnHandle> columnHandles)
    {
        return FluentIterable.from(columnHandles).filter(new Predicate<RaptorColumnHandle>()
        {
            @Override
            public boolean apply(RaptorColumnHandle input)
            {
                return input.isBucketKey();
            }
        }).toList();
    }
}
