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

import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.TableMetadata;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.IDBI;

import java.io.IOException;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrganizationJobFactory
        implements JobFactory
{
    private static final Logger log = Logger.get(OrganizationJobFactory.class);

    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;

    @Inject
    public OrganizationJobFactory(@ForMetadata IDBI dbi, ShardManager shardManager, ShardCompactor compactor)
    {
        requireNonNull(dbi, "dbi is null");
        this.metadataDao = onDemandDao(dbi, MetadataDao.class);
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
    }

    @Override
    public Runnable create(OrganizationSet organizationSet)
    {
        return new OrganizationJob(organizationSet);
    }

    private class OrganizationJob
            implements Runnable
    {
        private final OrganizationSet organizationSet;

        public OrganizationJob(OrganizationSet organizationSet)
        {
            this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
        }

        @Override
        public void run()
        {
            try {
                runJob(organizationSet.getTableId(), organizationSet.getBucketNumber(), organizationSet.getShards());
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
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
            shardManager.replaceShardUuids(transactionId, tableId, metadata.getColumns(), shardUuids, newShards);
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
}
