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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.DuckLakeTableHandle;
import com.facebook.presto.ducklake.DuckLakeTableLayoutHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedTable;
import com.facebook.presto.ducklake.reader.delete.DeleteFilterPageSource;
import com.facebook.presto.ducklake.reader.delete.PositionDeleteFilter;
import com.facebook.presto.ducklake.split.DeleteFile;
import com.facebook.presto.ducklake.split.DuckLakeSplit;
import com.facebook.presto.ducklake.split.DuckLakeSplitKind;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeColumnHandle.ROW_POSITION_COLUMN_HANDLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Builds the read path for one split. Parquet data files are delegated to {@link
 * ParquetPageSourceFactory}; a table's inlined rows (spec &sect;3) are streamed from the catalog
 * database by {@link InlinedDataPageSource}, opened through {@link
 * DuckLakeCatalog#openInlinedRows}. When a split carries a positional delete file (spec
 * &sect;4.5), it is read eagerly into a {@link PositionDeleteFilter} and, unless empty, wrapped
 * around the data page source through {@link DeleteFilterPageSource}, appending the {@code
 * $row_position} column to the requested columns when the query did not already ask for it. When
 * the split's data file is only partially visible at the query snapshot ({@code partial_max}
 * greater than the snapshot, meaning the file was produced by {@code
 * ducklake_merge_adjacent_files} out of inserts from more than one snapshot), the hidden per-row
 * snapshot column is requested too and the data page source is wrapped in a {@link
 * SnapshotFilterPageSource}. Neither wrapper ever applies to an inlined split: deletes and
 * compaction are both properties of a Parquet data file, and {@link DuckLakeSplit#getPartialMax()}
 * is always empty for one.
 */
public class DuckLakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    /**
     * Parquet field id of the hidden {@code _ducklake_internal_snapshot_id} column DuckDB stamps
     * onto every row of a compacted data file (and of a delete file that embeds per-row snapshot
     * visibility), fixed by the DuckLake spec (&sect;4.5) independent of table schema.
     */
    private static final DuckLakeColumnHandle SNAPSHOT_ID_COLUMN_HANDLE =
            DuckLakeColumnHandle.primitiveColumnHandle(2147483539L, "_ducklake_internal_snapshot_id", "int64", BIGINT);

    /**
     * The delete file's Parquet field ids, fixed by the DuckLake spec (&sect;4.5) independent of
     * table schema: {@code file_path} and {@code pos} are always present; {@code
     * _ducklake_internal_snapshot_id} appears only in a delete file that embeds per-row snapshot
     * visibility. {@code file_path} is requested (and ignored) only so the column list lines up
     * positionally with {@link PositionDeleteFilter#accumulate}'s channel expectations.
     */
    private static final List<DuckLakeColumnHandle> DELETE_FILE_COLUMNS = ImmutableList.of(
            DuckLakeColumnHandle.primitiveColumnHandle(2147483646L, "file_path", "varchar", VARCHAR),
            DuckLakeColumnHandle.primitiveColumnHandle(2147483645L, "pos", "int64", BIGINT),
            SNAPSHOT_ID_COLUMN_HANDLE);

    private final ParquetPageSourceFactory parquetPageSourceFactory;
    private final DuckLakeCatalog catalog;

    @Inject
    public DuckLakePageSourceProvider(ParquetPageSourceFactory parquetPageSourceFactory, DuckLakeCatalog catalog)
    {
        this.parquetPageSourceFactory = requireNonNull(parquetPageSourceFactory, "parquetPageSourceFactory is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext,
            RuntimeStats runtimeStats)
    {
        DuckLakeSplit duckLakeSplit = (DuckLakeSplit) split;
        if (duckLakeSplit.getKind() == DuckLakeSplitKind.INLINED) {
            return createInlinedPageSource(duckLakeSplit, columns);
        }

        DuckLakeTableLayoutHandle layoutHandle = (DuckLakeTableLayoutHandle) layout;
        DuckLakeTableHandle table = layoutHandle.getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName().getTableName();
        List<DuckLakeColumnHandle> duckLakeColumns = columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .collect(toImmutableList());
        TupleDomain<DuckLakeColumnHandle> effectivePredicate = layoutHandle.getDomainPredicate().transform(DuckLakeColumnHandle.class::cast);

        Optional<PositionDeleteFilter> filter = readDeleteFilter(session, duckLakeSplit, schemaName, tableName);
        boolean needsSnapshotFilter = needsPartialFileSnapshotFilter(duckLakeSplit);
        if (!filter.isPresent() && !needsSnapshotFilter) {
            return parquetPageSourceFactory.createPageSource(session, duckLakeSplit, schemaName, tableName, duckLakeColumns, effectivePredicate);
        }

        int positionChannel = rowPositionChannel(duckLakeColumns);
        boolean appendedPositionChannel = filter.isPresent() && positionChannel < 0;
        List<DuckLakeColumnHandle> dataColumns = duckLakeColumns;
        if (appendedPositionChannel) {
            dataColumns = ImmutableList.<DuckLakeColumnHandle>builder()
                    .addAll(dataColumns)
                    .add(ROW_POSITION_COLUMN_HANDLE)
                    .build();
            positionChannel = dataColumns.size() - 1;
        }

        // The snapshot column, when needed, is appended last so that dropping it (always the
        // final channel of the page SnapshotFilterPageSource sees) never shifts positionChannel,
        // which was fixed above against the column list as it stood before this append.
        int snapshotChannel = -1;
        if (needsSnapshotFilter) {
            dataColumns = ImmutableList.<DuckLakeColumnHandle>builder()
                    .addAll(dataColumns)
                    .add(SNAPSHOT_ID_COLUMN_HANDLE)
                    .build();
            snapshotChannel = dataColumns.size() - 1;
        }

        ConnectorPageSource dataPageSource = parquetPageSourceFactory.createPageSource(session, duckLakeSplit, schemaName, tableName, dataColumns, effectivePredicate);
        if (needsSnapshotFilter) {
            dataPageSource = new SnapshotFilterPageSource(dataPageSource, duckLakeSplit.getSnapshotId(), snapshotChannel);
        }
        if (filter.isPresent()) {
            dataPageSource = new DeleteFilterPageSource(dataPageSource, filter.get(), positionChannel, appendedPositionChannel);
        }
        return dataPageSource;
    }

    /**
     * Opens the split's inlined table over JDBC and streams it through {@link
     * InlinedDataPageSource}. The schema version {@link DuckLakeInlinedTable} otherwise carries is
     * irrelevant here: {@link DuckLakeCatalog#openInlinedRows} only ever uses the table name.
     */
    private ConnectorPageSource createInlinedPageSource(DuckLakeSplit split, List<ColumnHandle> columns)
    {
        List<DuckLakeColumnHandle> duckLakeColumns = columns.stream()
                .map(DuckLakeColumnHandle.class::cast)
                .collect(toImmutableList());
        List<String> columnNames = InlinedDataPageSource.regularColumnNames(duckLakeColumns);
        DuckLakeInlinedTable inlinedTable = new DuckLakeInlinedTable(
                split.getInlinedTableName().orElseThrow(() -> new IllegalStateException("INLINED split has no inlinedTableName: " + split)),
                0);
        DuckLakeInlinedRowSource rowSource = catalog.openInlinedRows(split.getSnapshotId(), inlinedTable, columnNames);
        return new InlinedDataPageSource(rowSource, duckLakeColumns, InlinedDataPageSource.DEFAULT_BATCH_SIZE);
    }

    /**
     * True when this split's data file was produced by {@code ducklake_merge_adjacent_files} out
     * of inserts from more than one snapshot ({@code partial_max} set) and the query is reading at
     * a snapshot below that maximum, so some of the file's rows were not yet inserted at the query
     * snapshot and must be filtered out by {@link SnapshotFilterPageSource}. False for an ordinary
     * (non-compacted) file, for an inlined split (whose {@code partialMax} is always empty), and
     * for a read at the latest snapshot or any snapshot at or above {@code partial_max}.
     */
    private static boolean needsPartialFileSnapshotFilter(DuckLakeSplit split)
    {
        return split.getPartialMax().isPresent() && split.getPartialMax().getAsLong() > split.getSnapshotId();
    }

    /**
     * Reads every delete file attached to the split, if any, into a single {@link
     * PositionDeleteFilter}. DuckLake attaches at most one delete file to a split in practice, but
     * {@link DuckLakeSplit#getDeletes()} is a list (kept Iceberg-shaped for a later native
     * translation), so every entry is opened and unioned into one shared bitmap rather than only
     * looking at the first: a split manager change that ever attached more than one must not
     * silently lose the extra file's deletes. Returns empty both when the split has no delete file
     * and when none of the delete files' positions apply to this split's snapshot, so the caller
     * can skip wrapping the data page source entirely.
     */
    private Optional<PositionDeleteFilter> readDeleteFilter(ConnectorSession session, DuckLakeSplit split, String schemaName, String tableName)
    {
        List<DeleteFile> deletes = split.getDeletes();
        if (deletes.isEmpty()) {
            return Optional.empty();
        }
        LongBitmapDataProvider deletedPositions = new Roaring64Bitmap();
        for (DeleteFile delete : deletes) {
            ConnectorPageSource deleteSource = parquetPageSourceFactory.openParquetFile(
                    session, delete.getPath(), delete.getFileSizeInBytes(), DELETE_FILE_COLUMNS, schemaName, tableName);
            PositionDeleteFilter.accumulate(deleteSource, split.getSnapshotId(), delete.getPath(), deletedPositions);
        }
        PositionDeleteFilter filter = PositionDeleteFilter.of(deletedPositions);
        return filter.isEmpty() ? Optional.empty() : Optional.of(filter);
    }

    private static int rowPositionChannel(List<DuckLakeColumnHandle> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isRowPositionColumn()) {
                return i;
            }
        }
        return -1;
    }
}
