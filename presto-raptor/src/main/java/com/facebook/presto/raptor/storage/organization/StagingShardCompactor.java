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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.filesystem.FileSystemContext;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.ReaderAttributes;
import com.facebook.presto.raptor.storage.StagingStorageManager;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class StagingShardCompactor
        extends ShardCompactor
{
    private static final ReaderAttributes SMALL_READ_ATTRIBUTES = new ReaderAttributes(
            new DataSize(1, BYTE),
            new DataSize(100, KILOBYTE),
            new DataSize(100, KILOBYTE),
            new DataSize(1, BYTE),
            false,
            false);

    private final StorageManager orcStorageManager;
    private final StagingStorageManager stagingStorageManager;
    private final ReaderAttributes readerAttributes;

    @Inject
    public StagingShardCompactor(StorageManager orcStorageManager, StagingStorageManager stagingStorageManager, ReaderAttributes readerAttributes)
    {
        super(orcStorageManager, readerAttributes);
        this.orcStorageManager = requireNonNull(orcStorageManager, "storageManager is null");
        this.stagingStorageManager = requireNonNull(stagingStorageManager, "storageManager is null");
        this.readerAttributes = requireNonNull(readerAttributes, "readerAttributes is null");
    }

    @Override
    public List<ShardInfo> compactSorted(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, List<SortOrder> sortOrders)
            throws IOException
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");

        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        checkArgument(columnIds.containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");

        // staged compaction
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = orcStorageManager.getPageSource(FileSystemContext.DEFAULT_RAPTOR_CONTEXT, uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), readerAttributes)) {
                StoragePageSink storagePageSink = null;
                try {
                    storagePageSink = stagingStorageManager.createStoragePageSink(columnIds, columnTypes, uuid);
                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (isNullOrEmptyPage(page)) {
                            continue;
                        }
                        storagePageSink.appendPages(ImmutableList.of(page));
                    }
                    storagePageSink.commit();
                }
                catch (RuntimeException e) {
                    if (storagePageSink != null) {
                        storagePageSink.rollback();
                    }
                    throw e;
                }
            }
        }

        List<ShardInfo> shardInfos = compactSorted(stagingStorageManager, SMALL_READ_ATTRIBUTES, transactionId, bucketNumber, uuids, columns, sortColumnIds, sortOrders);

        stagingStorageManager.deleteStagingShards(uuids);

        return shardInfos;
    }
}
