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
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.changelog.ChangelogSplitSource;
import com.facebook.presto.iceberg.changelog.ChangelogUtil;
import com.facebook.presto.iceberg.samples.SampleUtil;
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
import org.apache.iceberg.util.TableScanUtil;

import javax.inject.Inject;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.CatalogType.NESSIE;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.TableType.CHANGELOG;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final IcebergTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergResourceFactory resourceFactory;
    private final CatalogType catalogType;
    private final TypeManager typeManager;

    @Inject
    public IcebergSplitManager(
            IcebergConfig config,
            IcebergResourceFactory resourceFactory,
            IcebergTransactionManager transactionManager,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        requireNonNull(config, "config is null");
        this.catalogType = config.getCatalogType();
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

        if (!table.getSnapshotId().isPresent()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        Table icebergTable;
        if (catalogType == HADOOP || catalogType == NESSIE) {
            icebergTable = getNativeIcebergTable(resourceFactory, session, table.getSchemaTableName());
        }
        else {
            ExtendedHiveMetastore metastore = ((IcebergHiveMetadata) transactionManager.get(transaction)).getMetastore();
            icebergTable = getHiveIcebergTable(metastore, hdfsEnvironment, session, table.getSchemaTableName());
        }
        if (table.getTableType() == TableType.SAMPLES) {
            icebergTable = SampleUtil.getSampleTableFromActual(icebergTable, table.getSchemaName(), hdfsEnvironment, session);
        }

        if (table.getTableType() == CHANGELOG) {
            // TODO change this to calculate the correct id to scan from.
            // this change only reads the diff between the current and immediate parent snapshot.
            long fromSnap = Long.parseLong(icebergTable.properties().get(IcebergTableProperties.SAMPLE_TABLE_LAST_SNAPSHOT));
            String primaryKeyColumnName = icebergTable.properties().get(IcebergTableProperties.SAMPLE_TABLE_PRIMARY_KEY);
            // if the predicate is on the "primary_key" column, then we can add a predicate to the table
            // scan. We need to convert the predicate to filter on the correct column name though.
            IncrementalChangelogScan scan = icebergTable.newIncrementalChangelogScan()
                    .filter(toIcebergExpression(ChangelogUtil.convertTupleDomain(table.getPredicate(), primaryKeyColumnName, icebergTable, typeManager)))
                    .fromSnapshotExclusive(fromSnap);
            return new ChangelogSplitSource(session, typeManager, icebergTable, scan, scan.targetSplitSize());
        }
        else {
            TableScan tableScan = icebergTable.newScan()
                    .filter(toIcebergExpression(table.getPredicate()))
                    .useSnapshot(table.getSnapshotId().get());

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
