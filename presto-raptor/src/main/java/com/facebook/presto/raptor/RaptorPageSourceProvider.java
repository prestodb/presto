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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.util.ConcatPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.hive.HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT;
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
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        RaptorSplit raptorSplit = (RaptorSplit) split;
        OptionalInt bucketNumber = raptorSplit.getBucketNumber();
        TupleDomain<RaptorColumnHandle> predicate = raptorSplit.getEffectivePredicate();
        ReaderAttributes attributes = ReaderAttributes.from(session);
        OptionalLong transactionId = raptorSplit.getTransactionId();
        Optional<Map<String, Type>> columnTypes = raptorSplit.getColumnTypes();
        boolean tableSupportsDeltaDelete = raptorSplit.isTableSupportsDeltaDelete();

        HdfsContext context = new HdfsContext(session);

        Map<UUID, UUID> shardDeltaMap = raptorSplit.getShardDeltaMap();
        if (raptorSplit.getShardUuids().size() == 1) {
            UUID shardUuid = raptorSplit.getShardUuids().iterator().next();
            return createPageSource(
                    context,
                    DEFAULT_HIVE_FILE_CONTEXT,
                    shardUuid,
                    Optional.ofNullable(shardDeltaMap.get(shardUuid)),
                    tableSupportsDeltaDelete,
                    bucketNumber,
                    columns,
                    predicate,
                    attributes,
                    transactionId,
                    columnTypes);
        }

        Iterator<ConnectorPageSource> iterator = raptorSplit.getShardUuids().stream()
                .map(shardUuid -> createPageSource(
                        context,
                        DEFAULT_HIVE_FILE_CONTEXT,
                        shardUuid,
                        Optional.ofNullable(shardDeltaMap.get(shardUuid)),
                        tableSupportsDeltaDelete,
                        bucketNumber,
                        columns,
                        predicate,
                        attributes,
                        transactionId,
                        columnTypes))
                .iterator();

        return new ConcatPageSource(iterator);
    }

    /**
     *
     * @param deltaShardUuid delta of one shard
     * @param tableSupportsDeltaDelete table property indicating if this table supports delta_delete
     * In the future, we could have the concept of delta_delete as session property.
     * Having these two parameters at the same time gives us the flexibility and compatibility to future features.
     */
    private ConnectorPageSource createPageSource(
            HdfsContext context,
            HiveFileContext hiveFileContext,
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

        return storageManager.getPageSource(context, hiveFileContext, shardUuid, deltaShardUuid, tableSupportsDeltaDelete, bucketNumber, columnIds, columnTypes, predicate, attributes, transactionId, allColumnTypes);
    }
}
