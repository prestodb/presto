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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.changelog.ChangelogSplitSource;
import com.facebook.presto.iceberg.equalitydeletes.EqualityDeletesSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

import javax.inject.Inject;

import static com.facebook.presto.hive.rule.FilterPushdownUtils.isEntireColumn;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergTableType.CHANGELOG;
import static com.facebook.presto.iceberg.IcebergTableType.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager,
            TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) layout;
        IcebergTableHandle table = layoutHandle.getTable();

        if (!table.getIcebergTableName().getSnapshotId().isPresent()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        TupleDomain<IcebergColumnHandle> predicate = isPushdownFilterEnabled(session) ?
                layoutHandle.getPartitionColumnPredicate()
                        .transform(IcebergColumnHandle.class::cast)
                        .intersect(layoutHandle.getDomainPredicate()
                                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                                .transform(layoutHandle.getPredicateColumns()::get)) :
                table.getPredicate();

        Table icebergTable = getIcebergTable(transactionManager.get(transaction), session, table.getSchemaTableName());

        if (table.getIcebergTableName().getTableType() == CHANGELOG) {
            // if the snapshot isn't specified, grab the oldest available version of the table
            long fromSnapshot = table.getIcebergTableName().getSnapshotId().orElseGet(() -> SnapshotUtil.oldestAncestor(icebergTable).snapshotId());
            long toSnapshot = table.getIcebergTableName().getChangelogEndSnapshot().orElse(icebergTable.currentSnapshot().snapshotId());
            IncrementalChangelogScan scan = icebergTable.newIncrementalChangelogScan()
                    .fromSnapshotExclusive(fromSnapshot)
                    .toSnapshot(toSnapshot);
            return new ChangelogSplitSource(session, typeManager, icebergTable, scan, scan.targetSplitSize());
        }
        else if (table.getIcebergTableName().getTableType() == EQUALITY_DELETES) {
            CloseableIterable<DeleteFile> deleteFiles = IcebergUtil.getDeleteFiles(icebergTable,
                    table.getIcebergTableName().getSnapshotId().get(),
                    table.getPredicate(),
                    table.getPartitionSpecId(),
                    table.getEqualityFieldIds());

            return new EqualityDeletesSplitSource(session, icebergTable, deleteFiles);
        }
        else {
            TableScan tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(predicate))
                    .useSnapshot(table.getIcebergTableName().getSnapshotId().get());

            // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
            //      propagate it at split level so the parquet pushdown can leverage it.
            IcebergSplitSource splitSource = new IcebergSplitSource(
                    session,
                    tableScan,
                    TableScanUtil.splitFiles(tableScan.planFiles(), tableScan.targetSplitSize()),
                    getMinimumAssignedSplitWeight(session));
            return splitSource;
        }
    }
}
