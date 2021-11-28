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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final IcebergTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final IcebergResourceFactory resourceFactory;
    private final boolean nativeCatalogMode;

    @Inject
    public IcebergSplitManager(
            IcebergConfig config,
            IcebergResourceFactory resourceFactory,
            IcebergTransactionManager transactionManager,
            HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
        requireNonNull(config, "config is null");
        this.nativeCatalogMode = config.isNativeMode();
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
        if (nativeCatalogMode) {
            icebergTable = resourceFactory.getCatalog(session).loadTable(toIcebergTableIdentifier(table.getSchemaTableName()));
        }
        else {
            ExtendedHiveMetastore metastore = ((IcebergMetadata) transactionManager.get(transaction)).getMetastore();
            icebergTable = getIcebergTable(metastore, hdfsEnvironment, session, table.getSchemaTableName());
        }

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(table.getPredicate()))
                .useSnapshot(table.getSnapshotId().get());

        // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
        //      propagate it at split level so the parquet pushdown can leverage it.
        IcebergSplitSource splitSource = new IcebergSplitSource(session, tableScan.planTasks());
        return splitSource;
    }
}
