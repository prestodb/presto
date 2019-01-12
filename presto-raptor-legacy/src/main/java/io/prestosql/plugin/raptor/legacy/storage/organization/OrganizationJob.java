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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import io.airlift.log.Logger;
import io.prestosql.plugin.raptor.legacy.metadata.ColumnInfo;
import io.prestosql.plugin.raptor.legacy.metadata.MetadataDao;
import io.prestosql.plugin.raptor.legacy.metadata.ShardInfo;
import io.prestosql.plugin.raptor.legacy.metadata.ShardManager;
import io.prestosql.plugin.raptor.legacy.metadata.TableColumn;
import io.prestosql.plugin.raptor.legacy.metadata.TableMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class OrganizationJob
        implements Runnable
{
    private static final Logger log = Logger.get(OrganizationJob.class);

    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;
    private final OrganizationSet organizationSet;

    public OrganizationJob(OrganizationSet organizationSet, MetadataDao metadataDao, ShardManager shardManager, ShardCompactor compactor)
    {
        this.metadataDao = requireNonNull(metadataDao, "metadataDao is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
    }

    @Override
    public void run()
    {
        try {
            runJob(organizationSet.getTableId(), organizationSet.getBucketNumber(), organizationSet.getShards());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runJob(long tableId, OptionalInt bucketNumber, Set<UUID> shardUuids)
            throws IOException
    {
        long transactionId = shardManager.beginTransaction();
        try {
            runJob(transactionId, bucketNumber, tableId, shardUuids);
        }
        catch (Throwable e) {
            shardManager.rollbackTransaction(transactionId);
            throw e;
        }
    }

    private void runJob(long transactionId, OptionalInt bucketNumber, long tableId, Set<UUID> shardUuids)
            throws IOException
    {
        TableMetadata metadata = getTableMetadata(tableId);
        List<ShardInfo> newShards = performCompaction(transactionId, bucketNumber, shardUuids, metadata);
        log.info("Compacted shards %s into %s", shardUuids, newShards.stream().map(ShardInfo::getShardUuid).collect(toList()));
        shardManager.replaceShardUuids(transactionId, tableId, metadata.getColumns(), shardUuids, newShards, OptionalLong.empty());
    }

    private TableMetadata getTableMetadata(long tableId)
    {
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);

        List<Long> sortColumnIds = sortColumns.stream()
                .map(TableColumn::getColumnId)
                .collect(toList());

        List<ColumnInfo> columns = metadataDao.listTableColumns(tableId).stream()
                .map(TableColumn::toColumnInfo)
                .collect(toList());
        return new TableMetadata(tableId, columns, sortColumnIds);
    }

    private List<ShardInfo> performCompaction(long transactionId, OptionalInt bucketNumber, Set<UUID> shardUuids, TableMetadata tableMetadata)
            throws IOException
    {
        if (tableMetadata.getSortColumnIds().isEmpty()) {
            return compactor.compact(transactionId, bucketNumber, shardUuids, tableMetadata.getColumns());
        }
        return compactor.compactSorted(
                transactionId,
                bucketNumber,
                shardUuids,
                tableMetadata.getColumns(),
                tableMetadata.getSortColumnIds(),
                nCopies(tableMetadata.getSortColumnIds().size(), ASC_NULLS_FIRST));
    }
}
