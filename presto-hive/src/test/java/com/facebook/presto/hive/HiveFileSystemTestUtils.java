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
package com.facebook.presto.hive;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.AbstractTestHiveClient.HiveTransaction;
import com.facebook.presto.hive.AbstractTestHiveClient.Transaction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.AbstractTestHiveClient.getAllSplits;
import static com.facebook.presto.hive.AbstractTestHiveFileSystem.SPLIT_SCHEDULING_CONTEXT;
import static com.facebook.presto.hive.HiveTestUtils.getAllSessionProperties;
import static com.facebook.presto.hive.HiveTestUtils.getTypes;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.testing.MaterializedResult.materializeSourceDataStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class HiveFileSystemTestUtils
{
    private HiveFileSystemTestUtils() {}

    public static MaterializedResult readTable(SchemaTableName tableName,
            HiveTransactionManager transactionManager,
            HiveClientConfig config,
            HiveMetadataFactory metadataFactory,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorSplitManager splitManager)
            throws IOException
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager, metadataFactory.get())) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);
            List<ColumnHandle> columnHandles = ImmutableList.copyOf(metadata.getColumnHandles(session, table).values());
            ConnectorTableLayoutResult tableLayoutResult = metadata.getTableLayoutForConstraint(session, table, Constraint.alwaysTrue(), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutResult.getTableLayout().getHandle();
            TableHandle tableHandle = new TableHandle(new ConnectorId(tableName.getSchemaName()), table, transaction.getTransactionHandle(), Optional.of(layoutHandle));

            metadata.beginQuery(session);

            splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, tableHandle.getLayout().get(), SPLIT_SCHEDULING_CONTEXT);

            List<Type> allTypes = getTypes(columnHandles);
            List<Type> dataTypes = getTypes(columnHandles.stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toImmutableList()));
            MaterializedResult.Builder result = MaterializedResult.resultBuilder(session, dataTypes);

            List<ConnectorSplit> splits = getAllSplits(splitSource);
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                        transaction.getTransactionHandle(),
                        session,
                        split,
                        tableHandle.getLayout().get(),
                        columnHandles,
                        NON_CACHEABLE,
                        new RuntimeStats())) {
                    MaterializedResult pageSourceResult = materializeSourceDataStream(session, pageSource, allTypes);
                    for (MaterializedRow row : pageSourceResult.getMaterializedRows()) {
                        Object[] dataValues = IntStream.range(0, row.getFieldCount())
                                .filter(channel -> !((HiveColumnHandle) columnHandles.get(channel)).isHidden())
                                .mapToObj(row::getField)
                                .toArray();
                        result.row(dataValues);
                    }
                }
            }
            return result.build();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    public static MaterializedResult filterTable(SchemaTableName tableName,
            List<ColumnHandle> projectedColumns,
            HiveTransactionManager transactionManager,
            HiveClientConfig config,
            HiveMetadataFactory metadataFactory,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorSplitManager splitManager)
            throws IOException
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager, metadataFactory.get())) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);
            ConnectorTableLayoutResult tableLayoutResult = metadata.getTableLayoutForConstraint(session, table, Constraint.alwaysTrue(), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutResult.getTableLayout().getHandle();
            TableHandle tableHandle = new TableHandle(new ConnectorId(tableName.getSchemaName()), table, transaction.getTransactionHandle(), Optional.of(layoutHandle));

            metadata.beginQuery(session);
            splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, tableHandle.getLayout().get(), SPLIT_SCHEDULING_CONTEXT);

            List<Type> allTypes = getTypes(projectedColumns);
            List<Type> dataTypes = getTypes(projectedColumns.stream()
                    .filter(columnHandle -> !((HiveColumnHandle) columnHandle).isHidden())
                    .collect(toImmutableList()));
            MaterializedResult.Builder result = MaterializedResult.resultBuilder(session, dataTypes);

            List<ConnectorSplit> splits = getAllSplits(splitSource);
            for (ConnectorSplit split : splits) {
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                        transaction.getTransactionHandle(),
                        session,
                        split,
                        tableHandle.getLayout().get(),
                        projectedColumns,
                        NON_CACHEABLE,
                        new RuntimeStats())) {
                    MaterializedResult pageSourceResult = materializeSourceDataStream(session, pageSource, allTypes);
                    for (MaterializedRow row : pageSourceResult.getMaterializedRows()) {
                        Object[] dataValues = IntStream.range(0, row.getFieldCount())
                                .filter(channel -> !((HiveColumnHandle) projectedColumns.get(channel)).isHidden())
                                .mapToObj(row::getField)
                                .toArray();
                        result.row(dataValues);
                    }
                }
            }
            return result.build();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    public static int getSplitsCount(SchemaTableName tableName,
            HiveTransactionManager transactionManager,
            HiveClientConfig config,
            HiveMetadataFactory metadataFactory,
            ConnectorSplitManager splitManager)
    {
        ConnectorMetadata metadata = null;
        ConnectorSession session = null;
        ConnectorSplitSource splitSource = null;

        try (Transaction transaction = newTransaction(transactionManager, metadataFactory.get())) {
            metadata = transaction.getMetadata();
            session = newSession(config);

            ConnectorTableHandle table = getTableHandle(metadata, tableName, session);
            ConnectorTableLayoutResult tableLayoutResult = metadata.getTableLayoutForConstraint(session, table, Constraint.alwaysTrue(), Optional.empty());
            HiveTableLayoutHandle layoutHandle = (HiveTableLayoutHandle) tableLayoutResult.getTableLayout().getHandle();
            TableHandle tableHandle = new TableHandle(new ConnectorId(tableName.getSchemaName()), table, transaction.getTransactionHandle(), Optional.of(layoutHandle));

            metadata.beginQuery(session);
            splitSource = splitManager.getSplits(transaction.getTransactionHandle(), session, tableHandle.getLayout().get(), SPLIT_SCHEDULING_CONTEXT);

            return getAllSplits(splitSource).size();
        }
        finally {
            cleanUpQuery(metadata, session);
            closeQuietly(splitSource);
        }
    }

    public static Transaction newTransaction(HiveTransactionManager transactionManager, HiveMetadata hiveMetadata)
    {
        return new HiveTransaction(transactionManager, hiveMetadata);
    }

    public static ConnectorSession newSession(HiveClientConfig config)
    {
        return new TestingConnectorSession(getAllSessionProperties(config, new HiveCommonClientConfig()));
    }

    public static ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName, ConnectorSession session)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(session, tableName);
        checkArgument(handle != null, "table not found: %s", tableName);
        return handle;
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static void cleanUpQuery(ConnectorMetadata metadata, ConnectorSession session)
    {
        if (metadata != null && session != null) {
            metadata.cleanupQuery(session);
        }
    }
}
