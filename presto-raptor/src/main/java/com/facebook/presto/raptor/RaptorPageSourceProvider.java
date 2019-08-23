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

import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.util.ConcatPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StorageManager storageManager;

    @Inject
    public RaptorPageSourceProvider(StorageManager storageManager)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        RaptorSplit raptorSplit = (RaptorSplit) split;

        OptionalInt bucketNumber = raptorSplit.getBucketNumber();
        TupleDomain<RaptorColumnHandle> predicate = raptorSplit.getEffectivePredicate();
        ReaderAttributes attributes = ReaderAttributes.from(session);
        OptionalLong transactionId = raptorSplit.getTransactionId();
        Optional<Map<String, Type>> columnTypes = raptorSplit.getColumnTypes();
        boolean tableSupportsDeltaDelete = raptorSplit.isTableSupportsDeltaDelete();

        FileSystemContext context = new FileSystemContext(session);

        Map<UUID, UUID> shardDeltaMap = raptorSplit.getShardDeltaMap();
        if (raptorSplit.getShardUuids().size() == 1) {
            UUID shardUuid = raptorSplit.getShardUuids().iterator().next();
            return createPageSource(context, shardUuid,
                    Optional.ofNullable(shardDeltaMap.get(shardUuid)),
                    tableSupportsDeltaDelete, bucketNumber, columns, predicate, attributes, transactionId, columnTypes);
        }

        Iterator<ConnectorPageSource> iterator = raptorSplit.getShardUuids().stream()
                .map(shardUuid -> createPageSource(context, shardUuid,
                        Optional.ofNullable(shardDeltaMap.get(shardUuid)),
                        tableSupportsDeltaDelete, bucketNumber, columns, predicate, attributes, transactionId, columnTypes))
                .iterator();

        return new ConcatPageSource(iterator);
    }

    private ConnectorPageSource createPageSource(
            FileSystemContext context,
            UUID shardUuid,
            Optional<UUID> deltaShardUuid,
            boolean tableSupportsDeltaDelete,
            OptionalInt bucketNumber,
            List<ColumnHandle> columns,
            TupleDomain<RaptorColumnHandle> predicate,
            ReaderAttributes attributes,
            OptionalLong transactionId,
            Optional<Map<String, Type>> allColumnTypes)
    {
        List<RaptorColumnHandle> columnHandles = columns.stream().map(RaptorColumnHandle.class::cast).collect(toList());
        List<Long> columnIds = columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
        List<Type> columnTypes = columnHandles.stream().map(RaptorColumnHandle::getColumnType).collect(toList());

        return storageManager.getPageSource(context, shardUuid, deltaShardUuid, tableSupportsDeltaDelete, bucketNumber, columnIds, columnTypes, predicate, attributes, transactionId, allColumnTypes);
    }
}
