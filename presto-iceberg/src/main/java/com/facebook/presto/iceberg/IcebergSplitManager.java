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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.changelog.ChangelogSplitSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

import javax.inject.Inject;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.TableType.CHANGELOG;
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

        if (!table.getTableName().getSnapshotId().isPresent()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        Table icebergTable = getIcebergTable(transactionManager.get(transaction), session, table.getSchemaTableName());

        if (table.getTableName().getTableType() == CHANGELOG) {
            // if the snapshot isn't specified, grab the oldest available version of the table
            long fromSnapshot = table.getTableName().getSnapshotId().orElseGet(() -> SnapshotUtil.oldestAncestor(icebergTable).snapshotId());
            long toSnapshot = table.getTableName().getChangelogEndSnapshot().orElse(icebergTable.currentSnapshot().snapshotId());
            IncrementalChangelogScan scan = icebergTable.newIncrementalChangelogScan()
                    .fromSnapshotExclusive(fromSnapshot)
                    .toSnapshot(toSnapshot);
            return new ChangelogSplitSource(session, typeManager, icebergTable, scan, scan.targetSplitSize());
        }
        else {
            TableScan tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(table.getPredicate()))
                    .useSnapshot(table.getTableName().getSnapshotId().get());

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
