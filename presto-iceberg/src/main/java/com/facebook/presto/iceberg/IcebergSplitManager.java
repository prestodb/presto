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

import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
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
import jakarta.inject.Inject;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergTableType.CHANGELOG;
import static com.facebook.presto.iceberg.IcebergTableType.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getMetadataColumnConstraints;
import static com.facebook.presto.iceberg.IcebergUtil.getNonMetadataColumnConstraints;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorServiceMBean;

    @Inject
    public IcebergSplitManager(
            IcebergTransactionManager transactionManager,
            TypeManager typeManager,
            @ForIcebergSplitManager ExecutorService executor)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.executorServiceMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
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

        TupleDomain<IcebergColumnHandle> predicate = getNonMetadataColumnConstraints(layoutHandle
                .getValidPredicate());

        Table icebergTable = getIcebergTable(transactionManager.get(transaction), session, table.getSchemaTableName());

        if (table.getIcebergTableName().getTableType() == CHANGELOG) {
            // if the snapshot isn't specified, grab the oldest available version of the table
            long fromSnapshot = table.getIcebergTableName().getSnapshotId().orElseGet(() -> SnapshotUtil.oldestAncestor(icebergTable).snapshotId());
            long toSnapshot = table.getIcebergTableName().getChangelogEndSnapshot()
                    .orElseGet(icebergTable.currentSnapshot()::snapshotId);
            IncrementalChangelogScan scan = icebergTable.newIncrementalChangelogScan()
                    .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()))
                    .fromSnapshotExclusive(fromSnapshot)
                    .toSnapshot(toSnapshot);
            return new ChangelogSplitSource(session, typeManager, icebergTable, scan);
        }
        else if (table.getIcebergTableName().getTableType() == EQUALITY_DELETES) {
            CloseableIterable<DeleteFile> deleteFiles = IcebergUtil.getDeleteFiles(icebergTable,
                    table.getIcebergTableName().getSnapshotId().get(),
                    predicate,
                    table.getPartitionSpecId(),
                    table.getEqualityFieldIds(),
                    session.getRuntimeStats());

            return new EqualityDeletesSplitSource(session, icebergTable, deleteFiles);
        }
        else {
            TableScan tableScan = icebergTable.newScan()
                    .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()))
                    .filter(toIcebergExpression(predicate))
                    .useSnapshot(table.getIcebergTableName().getSnapshotId().get())
                    .planWith(executor);

            // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
            //      propagate it at split level so the parquet pushdown can leverage it.
            IcebergSplitSource splitSource = new IcebergSplitSource(
                    session,
                    tableScan,
                    getMetadataColumnConstraints(layoutHandle.getValidPredicate()));
            return splitSource;
        }
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorServiceMBean;
    }
}
