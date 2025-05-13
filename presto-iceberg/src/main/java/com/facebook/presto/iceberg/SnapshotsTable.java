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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.iceberg.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.SqlTimestamp.MICROSECONDS_PER_MILLISECOND;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.buildTableScan;
import static com.facebook.presto.iceberg.IcebergUtil.columnNameToPositionInSchema;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.SNAPSHOTS;

public class SnapshotsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private static final String COMMITTED_AT_COLUMN_NAME = "committed_at";
    private static final String SNAPSHOT_ID_COLUMN_NAME = "snapshot_id";
    private static final String PARENT_ID_COLUMN_NAME = "parent_id";
    private static final String OPERATION_COLUMN_NAME = "operation";
    private static final String MANIFEST_LIST_COLUMN_NAME = "manifest_list";
    private static final String SUMMARY_COLUMN_NAME = "summary";

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(ColumnMetadata.builder().setName(COMMITTED_AT_COLUMN_NAME).setType(TIMESTAMP_WITH_TIME_ZONE).build())
                        .add(ColumnMetadata.builder().setName(SNAPSHOT_ID_COLUMN_NAME).setType(BIGINT).build())
                        .add(ColumnMetadata.builder().setName(PARENT_ID_COLUMN_NAME).setType(BIGINT).build())
                        .add(ColumnMetadata.builder().setName(OPERATION_COLUMN_NAME).setType(VARCHAR).build())
                        .add(ColumnMetadata.builder().setName(MANIFEST_LIST_COLUMN_NAME).setType(VARCHAR).build())
                        .add(ColumnMetadata.builder().setName(SUMMARY_COLUMN_NAME).setType(typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature()),
                                TypeSignatureParameter.of(VARCHAR.getTypeSignature())))).build())
                        .build());
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);
        TableScan tableScan = buildTableScan(icebergTable, SNAPSHOTS);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        Map<String, Integer> columnNameToPosition = columnNameToPositionInSchema(tableScan.schema());

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            fileScanTasks.forEach(fileScanTask -> addRows((DataTask) fileScanTask, pagesBuilder, timeZoneKey, columnNameToPosition));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return pagesBuilder.build();
    }

    private static boolean checkNonNull(Object object, PageListBuilder pagesBuilder)
    {
        if (object == null) {
            pagesBuilder.appendNull();
            return false;
        }
        return true;
    }

    private static void addRows(DataTask dataTask, PageListBuilder pagesBuilder, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        try (CloseableIterable<StructLike> dataRows = dataTask.rows()) {
            dataRows.forEach(dataTaskRow -> addRow(pagesBuilder, dataTaskRow, timeZoneKey, columnNameToPositionInSchema));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();

        pagesBuilder.appendTimestampTzMillis(
                structLike.get(columnNameToPositionInSchema.get(COMMITTED_AT_COLUMN_NAME), Long.class) / MICROSECONDS_PER_MILLISECOND,
                timeZoneKey);
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(SNAPSHOT_ID_COLUMN_NAME), Long.class));

        Long parentId = structLike.get(columnNameToPositionInSchema.get(PARENT_ID_COLUMN_NAME), Long.class);
        if (checkNonNull(parentId, pagesBuilder)) {
            pagesBuilder.appendBigint(parentId);
        }

        String operation = structLike.get(columnNameToPositionInSchema.get(OPERATION_COLUMN_NAME), String.class);
        if (checkNonNull(operation, pagesBuilder)) {
            pagesBuilder.appendVarchar(operation);
        }

        String manifestList = structLike.get(columnNameToPositionInSchema.get(MANIFEST_LIST_COLUMN_NAME), String.class);
        if (checkNonNull(manifestList, pagesBuilder)) {
            pagesBuilder.appendVarchar(manifestList);
        }

        Map<String, String> summary = structLike.get(columnNameToPositionInSchema.get(SUMMARY_COLUMN_NAME), Map.class);
        if (checkNonNull(summary, pagesBuilder)) {
            pagesBuilder.appendVarcharVarcharMap(summary);
        }

        pagesBuilder.endRow();
    }
}
