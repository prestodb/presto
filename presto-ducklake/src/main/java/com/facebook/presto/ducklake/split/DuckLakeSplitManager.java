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
package com.facebook.presto.ducklake.split;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.ducklake.DuckLakeSessionProperties;
import com.facebook.presto.ducklake.DuckLakeTableHandle;
import com.facebook.presto.ducklake.DuckLakeTableLayoutHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeDeleteFile;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedTable;
import com.facebook.presto.ducklake.split.pruning.DataFilePruner;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Locale;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Reads the layout's resolved table and snapshot, asks the catalog for the data files and
 * inlined tables visible at that snapshot, prunes the data files against the layout's {@code
 * domainPredicate} through {@link DataFilePruner} (spec &sect;4.5), and hands one split per
 * surviving data file plus one split per inlined table to a {@link DuckLakeSplitSource}. Inlined
 * tables are never partitioned (spec &sect;3), so they are unaffected by pruning.
 */
public class DuckLakeSplitManager
        implements ConnectorSplitManager
{
    private static final long TARGET_SPLIT_SIZE_BYTES = new DataSize(128, MEGABYTE).toBytes();

    private final DuckLakeCatalog catalog;

    @Inject
    public DuckLakeSplitManager(DuckLakeCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        DuckLakeTableLayoutHandle layoutHandle = (DuckLakeTableLayoutHandle) layout;
        DuckLakeTableHandle table = layoutHandle.getTable();
        long snapshotId = table.getSnapshotId();
        double minimumAssignedSplitWeight = DuckLakeSessionProperties.getMinimumAssignedSplitWeight(session);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(snapshotId, table.toDuckLakeTable());
        dataFiles = DataFilePruner.prune(dataFiles, table.getSchema().getPartitionFields(), layoutHandle.getDomainPredicate());
        List<DuckLakeInlinedTable> inlinedTables = catalog.listInlinedTables(table.getTableId());

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        for (DuckLakeDataFile dataFile : dataFiles) {
            splits.add(toSplit(dataFile, snapshotId, minimumAssignedSplitWeight));
        }
        for (DuckLakeInlinedTable inlinedTable : inlinedTables) {
            splits.add(DuckLakeSplit.inlinedSplit(inlinedTable.getTableName(), snapshotId, SplitWeight.standard()));
        }

        return new DuckLakeSplitSource(splits.build());
    }

    static DuckLakeSplit toSplit(DuckLakeDataFile dataFile, long snapshotId, double minimumAssignedSplitWeight)
    {
        if (dataFile.getEncryptionKey().isPresent()) {
            throw new PrestoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Encrypted DuckLake data files are not supported: " + dataFile.getPath());
        }
        if (dataFile.getMappingId().isPresent()) {
            throw new PrestoException(
                    DUCKLAKE_UNSUPPORTED_FEATURE,
                    "DuckLake data files with column name mappings (imported files) are not supported: " + dataFile.getPath());
        }
        if (!isParquet(dataFile.getFileFormat())) {
            throw new PrestoException(
                    DUCKLAKE_UNSUPPORTED_FEATURE,
                    format("DuckLake data file format '%s' is not supported, only Parquet is supported: %s", dataFile.getFileFormat(), dataFile.getPath()));
        }

        List<DeleteFile> deletes = dataFile.getDeleteFile()
                .map(deleteFile -> ImmutableList.of(toDeleteFile(deleteFile)))
                .orElseGet(ImmutableList::of);

        return DuckLakeSplit.parquetSplit(
                dataFile.getPath(),
                0,
                dataFile.getFileSizeBytes(),
                dataFile.getFileFormat().toUpperCase(Locale.ROOT),
                dataFile.getFileSizeBytes(),
                dataFile.getPartitionValues(),
                deletes,
                dataFile.getRowIdStart(),
                dataFile.getPartialMax(),
                snapshotId,
                computeSplitWeight(dataFile.getFileSizeBytes(), minimumAssignedSplitWeight));
    }

    static DeleteFile toDeleteFile(DuckLakeDeleteFile deleteFile)
    {
        if (deleteFile.getEncryptionKey().isPresent()) {
            throw new PrestoException(DUCKLAKE_UNSUPPORTED_FEATURE, "Encrypted DuckLake delete files are not supported: " + deleteFile.getPath());
        }
        if (!isParquet(deleteFile.getFormat())) {
            throw new PrestoException(
                    DUCKLAKE_UNSUPPORTED_FEATURE,
                    format("DuckLake delete file format '%s' is not supported, only Parquet is supported: %s", deleteFile.getFormat(), deleteFile.getPath()));
        }

        return new DeleteFile(
                deleteFile.getPath(),
                deleteFile.getFormat().toUpperCase(Locale.ROOT),
                deleteFile.getDeleteCount(),
                deleteFile.getFileSizeBytes(),
                deleteFile.getPartialMax());
    }

    private static boolean isParquet(String format)
    {
        return format.equalsIgnoreCase("parquet");
    }

    static SplitWeight computeSplitWeight(long fileSize, double minimumAssignedSplitWeight)
    {
        return SplitWeight.fromProportion(Math.min(Math.max((double) fileSize / TARGET_SPLIT_SIZE_BYTES, minimumAssignedSplitWeight), 1.0));
    }
}
