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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.PostScript;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.StreamReader;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.orc.AbstractOrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.facebook.presto.orc.DwrfEncryptionInfo.createDwrfEncryptionInfo;
import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.facebook.presto.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

abstract class AbstractOrcRecordReader<T extends StreamReader>
        implements Closeable
{
    protected final OrcAggregatedMemoryContext systemMemoryUsage;

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractOrcRecordReader.class).instanceSize();

    private final OrcDataSource orcDataSource;

    private final T[] streamReaders;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private final long maxBlockBytes;
    private final Optional<EncryptionLibrary> encryptionLibrary;
    private final Map<Integer, Integer> dwrfEncryptionGroupMap;
    private final Map<Integer, Slice> intermediateKeyMetadata;
    private long currentPosition;
    private long currentStripePosition;
    private int currentBatchSize;
    private int nextBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;
    private OrcAggregatedMemoryContext currentStripeSystemMemoryContext;
    private Optional<DwrfEncryptionInfo> dwrfEncryptionInfo = Optional.empty();

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private long filePosition;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private int currentRowGroup = -1;
    private int currentGroupRowCount;
    private int nextRowInGroup;

    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;

    private final Map<String, Slice> userMetadata;

    private final Optional<OrcWriteValidation> writeValidation;
    private final Optional<OrcWriteValidation.WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<OrcWriteValidation.StatisticsValidation> rowGroupStatisticsValidation;
    private final Optional<OrcWriteValidation.StatisticsValidation> stripeStatisticsValidation;
    private final Optional<OrcWriteValidation.StatisticsValidation> fileStatisticsValidation;

    private final RuntimeStats runtimeStats;

    public AbstractOrcRecordReader(
            Map<Integer, Type> includedColumns,
            Map<Integer, List<Subfield>> requiredSubfields,
            T[] streamReaders,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            Optional<EncryptionLibrary> encryptionLibrary,
            Map<Integer, Integer> dwrfEncryptionGroupMap,
            Map<Integer, Slice> columnToIntermediateKeyMap,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            PostScript.HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            OrcAggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            StripeMetadataSource stripeMetadataSource,
            boolean cacheable,
            RuntimeStats runtimeStats)
    {
        requireNonNull(includedColumns, "includedColumns is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(types, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(encryptionLibrary, "encryptionLibrary is null");
        requireNonNull(dwrfEncryptionGroupMap, "dwrfEncryptionGroupMap is null");
        requireNonNull(columnToIntermediateKeyMap, "columnToIntermediateKeyMap is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(includedColumns));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.systemMemoryUsage = systemMemoryUsage;
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");

        // reduce the included columns to the set that is also present
        ImmutableSet.Builder<Integer> presentColumns = ImmutableSet.builder();
        OrcType root = types.get(0);
        for (int column : includedColumns.keySet()) {
            // an old file can have less columns since columns can be added
            // after the file was written
            if (column >= 0 && column < root.getFieldCount()) {
                presentColumns.add(column);
            }
        }
        this.presentColumns = presentColumns.build();

        this.maxBlockBytes = requireNonNull(maxBlockSize, "maxBlockSize is null").toBytes();

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = Optional.of(stripeStats.get(i));
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        Collections.sort(stripeInfos, comparingLong(info -> info.getStripe().getOffset()));

        long totalRowCount = 0;
        long fileRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        ImmutableList.Builder<Long> stripeFilePositions = ImmutableList.builder();
        if (predicate.matches(numberOfRows, getStatisticsByColumnOrdinal(root, fileStats))) {
            // select stripes that start within the specified split
            for (StripeInfo info : stripeInfos) {
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(root, stripe, info.getStats(), predicate)) {
                    stripes.add(stripe);
                    stripeFilePositions.add(fileRowCount);
                    totalRowCount += stripe.getNumberOfRows();
                }
                fileRowCount += stripe.getNumberOfRows();
            }
        }
        this.totalRowCount = totalRowCount;
        this.stripes = stripes.build();
        this.stripeFilePositions = stripeFilePositions.build();

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, maxMergeDistance, tinyStripeThreshold, systemMemoryUsage);
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.currentStripeSystemMemoryContext = this.systemMemoryUsage.newOrcAggregatedMemoryContext();

        Set<Integer> includedOrcColumns = getIncludedOrcColumns(types, this.presentColumns, requireNonNull(requiredSubfields, "requiredSubfields is null"));
        this.encryptionLibrary = encryptionLibrary;
        this.dwrfEncryptionGroupMap = ImmutableMap.copyOf(dwrfEncryptionGroupMap);
        this.intermediateKeyMetadata = createIntermediateKeysMap(columnToIntermediateKeyMap, dwrfEncryptionGroupMap, orcDataSource.getId());
        checkPermissionsForEncryptedColumns(includedOrcColumns, dwrfEncryptionGroupMap, intermediateKeyMetadata);

        stripeReader = new StripeReader(
                orcDataSource,
                decompressor,
                types,
                includedOrcColumns,
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                writeValidation,
                stripeMetadataSource,
                cacheable,
                this.dwrfEncryptionGroupMap,
                runtimeStats);

        this.streamReaders = requireNonNull(streamReaders, "streamReaders is null");
        for (int columnId = 0; columnId < root.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                checkArgument(streamReaders[columnId] != null, "Missing stream reader for column " + columnId);
            }
        }

        maxBytesPerCell = new long[streamReaders.length];

        OptionalInt fixedWidthRowSize = getFixedWidthRowSize(this.presentColumns, includedColumns);
        if (fixedWidthRowSize.isPresent()) {
            if (fixedWidthRowSize.getAsInt() == 0) {
                nextBatchSize = MAX_BATCH_SIZE;
            }
            else {
                nextBatchSize = adjustMaxBatchSize(MAX_BATCH_SIZE, maxBlockBytes, fixedWidthRowSize.getAsInt());
            }
        }
        else {
            nextBatchSize = initialBatchSize;
        }
    }

    private static Set<Integer> getIncludedOrcColumns(List<OrcType> types, Set<Integer> includedColumns, Map<Integer, List<Subfield>> requiredSubfields)
    {
        Set<Integer> includes = new LinkedHashSet<>();

        OrcType root = types.get(0);
        for (int includedColumn : includedColumns) {
            List<Subfield> subfields = Optional.ofNullable(requiredSubfields.get(includedColumn)).orElse(ImmutableList.of());
            includeOrcColumnsRecursive(types, includes, root.getFieldTypeIndex(includedColumn), subfields);
        }

        return includes;
    }

    private static void includeOrcColumnsRecursive(List<OrcType> types, Set<Integer> result, int typeId, List<Subfield> requiredSubfields)
    {
        result.add(typeId);
        OrcType type = types.get(typeId);

        Optional<Map<String, List<Subfield>>> requiredFields = Optional.empty();
        if (type.getOrcTypeKind() == STRUCT) {
            requiredFields = getRequiredFields(requiredSubfields);
        }

        int children = type.getFieldCount();
        for (int i = 0; i < children; ++i) {
            List<Subfield> subfields = ImmutableList.of();
            if (requiredFields.isPresent()) {
                String fieldName = type.getFieldNames().get(i).toLowerCase(Locale.ENGLISH);
                if (!requiredFields.get().containsKey(fieldName)) {
                    continue;
                }
                subfields = requiredFields.get().get(fieldName);
            }

            includeOrcColumnsRecursive(types, result, type.getFieldTypeIndex(i), subfields);
        }
    }

    private static Optional<Map<String, List<Subfield>>> getRequiredFields(List<Subfield> requiredSubfields)
    {
        if (requiredSubfields.isEmpty()) {
            return Optional.empty();
        }

        Map<String, List<Subfield>> fields = new HashMap<>();
        for (Subfield subfield : requiredSubfields) {
            List<Subfield.PathElement> path = subfield.getPath();
            String name = ((Subfield.NestedField) path.get(0)).getName().toLowerCase(Locale.ENGLISH);
            fields.computeIfAbsent(name, k -> new ArrayList<>());
            if (path.size() > 1) {
                fields.get(name).add(new Subfield("c", path.subList(1, path.size())));
            }
        }

        return Optional.of(ImmutableMap.copyOf(fields));
    }

    private static Map<Integer, Slice> createIntermediateKeysMap(
            Map<Integer, Slice> columnsToKeys,
            Map<Integer, Integer> dwrfEncryptionGroupMap,
            OrcDataSourceId dataSourceId)
    {
        Map<Integer, Slice> intermediateKeys = new HashMap<>(dwrfEncryptionGroupMap.values().size());
        for (Map.Entry<Integer, Slice> entry : columnsToKeys.entrySet()) {
            Slice key = entry.getValue();
            int orcColumn = entry.getKey();
            if (!dwrfEncryptionGroupMap.containsKey(orcColumn)) {
                // ignore columns that don't have encryption groups
                continue;
            }
            int group = dwrfEncryptionGroupMap.get(orcColumn);
            Slice previous = intermediateKeys.putIfAbsent(group, key);
            if (previous != null && !key.equals(previous)) {
                throw new OrcCorruptionException(dataSourceId, "intermediate keys mapping does not match encryption groups");
            }
        }
        return ImmutableMap.copyOf(intermediateKeys);
    }

    private void checkPermissionsForEncryptedColumns(Set<Integer> includedOrcColumns, Map<Integer, Integer> dwrfEncryptionGroupMap, Map<Integer, Slice> intermediateKeyMetadata)
    {
        for (Integer column : includedOrcColumns) {
            if (dwrfEncryptionGroupMap.containsKey(column) && !intermediateKeyMetadata.containsKey(dwrfEncryptionGroupMap.get(column))) {
                throw new OrcPermissionsException(orcDataSource.getId(), "No IEK provided to Decrypt column number %s", column);
            }
        }
    }

    private static OptionalInt getFixedWidthRowSize(Set<Integer> columns, Map<Integer, Type> columnTypes)
    {
        int totalFixedWidth = 0;
        for (int column : columns) {
            Type type = columnTypes.get(column);
            if (type instanceof FixedWidthType) {
                // add 1 byte for null flag
                totalFixedWidth += ((FixedWidthType) type).getFixedSize() + 1;
            }
            else {
                return OptionalInt.empty();
            }
        }

        return OptionalInt.of(totalFixedWidth);
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    protected void updateMaxCombinedBytesPerRow(int columnIndex, Block block)
    {
        if (block.getPositionCount() > 0) {
            long bytesPerCell = block.getSizeInBytes() / block.getPositionCount();
            if (maxBytesPerCell[columnIndex] < bytesPerCell) {
                maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[columnIndex] + bytesPerCell;
                maxBytesPerCell[columnIndex] = bytesPerCell;
                maxBatchSize = adjustMaxBatchSize(maxBatchSize, maxBlockBytes, maxCombinedBytesPerRow);
            }
        }
    }

    protected T[] getStreamReaders()
    {
        return streamReaders;
    }

    private static boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    private static boolean isStripeIncluded(
            OrcType rootStructType,
            StripeInformation stripe,
            Optional<StripeStatistics> stripeStats,
            OrcPredicate predicate)
    {
        // if there are no stats, include the column
        if (!stripeStats.isPresent()) {
            return true;
        }
        return predicate.matches(stripe.getNumberOfRows(), getStatisticsByColumnOrdinal(rootStructType, stripeStats.get().getColumnStatistics()));
    }

    @VisibleForTesting
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold, OrcAggregatedMemoryContext systemMemoryContext)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > tinyStripeThreshold.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, createTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold), systemMemoryContext.newOrcLocalMemoryContext(CachingOrcDataSource.class.getSimpleName()));
    }

    /**
     * Return the row position relative to the start of the file.
     */
    public long getFilePosition()
    {
        return filePosition;
    }

    /**
     * Returns the total number of rows in the file. This count includes rows
     * for stripes that were completely excluded due to stripe statistics.
     */
    public long getFileRowCount()
    {
        return fileRowCount;
    }

    /**
     * Return the row position within the stripes being read by this reader.
     * This position will include rows that were never read due to row groups
     * that are excluded due to row group statistics. Thus, it will advance
     * faster than the number of rows actually read.
     */
    public long getReaderPosition()
    {
        return currentPosition;
    }

    /**
     * Returns the total number of rows that can possibly be read by this reader.
     * This count may be fewer than the number of rows in the file if some
     * stripes were excluded due to stripe statistics, but may be more than
     * the number of rows read if some row groups are excluded due to statistics.
     */
    public long getReaderRowCount()
    {
        return totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(orcDataSource);
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    closer.register(column::close);
                }
            }
        }
        rowGroups = null;
        if (writeChecksumBuilder.isPresent()) {
            OrcWriteValidation.WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getStripeHash() == actualChecksum.getStripeHash(), "Invalid stripes checksum");
        }
        if (fileStatisticsValidation.isPresent()) {
            List<ColumnStatistics> columnStatistics = fileStatisticsValidation.get().build();
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), columnStatistics);
        }
    }

    public boolean isColumnPresent(int hiveColumnIndex)
    {
        return presentColumns.contains(hiveColumnIndex);
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        if (currentRowGroup >= 0) {
            if (rowGroupStatisticsValidation.isPresent()) {
                OrcWriteValidation.StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateRowGroupStatistics(orcDataSource.getId(), offset, currentRowGroup, statisticsValidation.build());
                statisticsValidation.reset();
            }
        }
        while (!rowGroups.hasNext() && currentStripe < stripes.size()) {
            advanceToNextStripe();
            currentRowGroup = -1;
        }

        if (!rowGroups.hasNext()) {
            currentGroupRowCount = 0;
            return false;
        }

        currentRowGroup++;
        RowGroup currentRowGroup = rowGroups.next();
        currentGroupRowCount = toIntExact(currentRowGroup.getRowCount());
        if (currentRowGroup.getMinAverageRowBytes() > 0) {
            maxBatchSize = adjustMaxBatchSize(maxBatchSize, maxBlockBytes, currentRowGroup.getMinAverageRowBytes());
        }

        currentPosition = currentStripePosition + currentRowGroup.getRowOffset();
        filePosition = stripeFilePositions.get(currentStripe) + currentRowGroup.getRowOffset();

        // give reader data streams from row group
        InputStreamSources rowGroupStreamSources = currentRowGroup.getStreamSources();
        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    private static int adjustMaxBatchSize(int maxBatchSize, long maxBlockBytes, long averageRowBytes)
    {
        return toIntExact(min(maxBatchSize, max(1, maxBlockBytes / averageRowBytes)));
    }

    protected int getNextRowInGroup()
    {
        return nextRowInGroup;
    }

    protected void batchRead(int batchSize)
    {
        nextRowInGroup += batchSize;
    }

    protected int prepareNextBatch()
            throws IOException
    {
        // update position for current row group (advancing resets them)
        filePosition += currentBatchSize;
        currentPosition += currentBatchSize;

        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                filePosition = fileRowCount;
                currentPosition = totalRowCount;
                return -1;
            }
        }

        // We will grow currentBatchSize by BATCH_SIZE_GROWTH_FACTOR starting from initialBatchSize to maxBatchSize or
        // the number of rows left in this rowgroup, whichever is smaller. maxBatchSize is adjusted according to the
        // block size for every batch and never exceed MAX_BATCH_SIZE. But when the number of rows in the last batch in
        // the current rowgroup is smaller than min(nextBatchSize, maxBatchSize), the nextBatchSize for next batch in
        // the new rowgroup should be grown based on min(nextBatchSize, maxBatchSize) but not by the number of rows in
        // the last batch, i.e. currentGroupRowCount - nextRowInGroup. For example, if the number of rows read for
        // single fixed width column are: 1, 16, 256, 1024, 1024,..., 1024, 256 and the 256 was because there is only
        // 256 rows left in this row group, then the nextBatchSize should be 1024 instead of 512. So we need to grow the
        // nextBatchSize before limiting the currentBatchSize by currentGroupRowCount - nextRowInGroup.
        currentBatchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(currentBatchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_BATCH_SIZE);
        currentBatchSize = toIntExact(min(currentBatchSize, currentGroupRowCount - nextRowInGroup));
        return currentBatchSize;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeSystemMemoryContext.close();
        currentStripeSystemMemoryContext = systemMemoryUsage.newOrcAggregatedMemoryContext();
        rowGroups = ImmutableList.<RowGroup>of().iterator();

        if (currentStripe >= 0) {
            if (stripeStatisticsValidation.isPresent()) {
                OrcWriteValidation.StatisticsValidation statisticsValidation = stripeStatisticsValidation.get();
                long offset = stripes.get(currentStripe).getOffset();
                writeValidation.get().validateStripeStatistics(orcDataSource.getId(), offset, statisticsValidation.build());
                statisticsValidation.reset();
            }
        }

        currentStripe++;
        if (currentStripe >= stripes.size()) {
            return;
        }

        if (currentStripe > 0) {
            currentStripePosition += stripes.get(currentStripe - 1).getNumberOfRows();
        }

        StripeInformation stripeInformation = stripes.get(currentStripe);
        validateWriteStripe(stripeInformation.getNumberOfRows());
        List<byte[]> stripeDecryptionKeyMetadata = getDecryptionKeyMetadata(currentStripe, stripes);

        // if there are encrypted columns and dwrfEncryptionInfo hasn't been set yet
        // or it has been set, but we have new decryption keys,
        // set dwrfEncryptionInfo
        if ((!stripeDecryptionKeyMetadata.isEmpty() && !dwrfEncryptionInfo.isPresent())
                || (dwrfEncryptionInfo.isPresent() && !stripeDecryptionKeyMetadata.equals(dwrfEncryptionInfo.get().getEncryptedKeyMetadatas()))) {
            verify(encryptionLibrary.isPresent(), "encryptionLibrary is absent");
            dwrfEncryptionInfo = Optional.of(createDwrfEncryptionInfo(encryptionLibrary.get(), stripeDecryptionKeyMetadata, intermediateKeyMetadata, dwrfEncryptionGroupMap));
        }

        SharedBuffer sharedDecompressionBuffer = new SharedBuffer(currentStripeSystemMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer"));
        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeSystemMemoryContext, dwrfEncryptionInfo, sharedDecompressionBuffer);
        if (stripe != null) {
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    column.startStripe(stripe);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
    }

    @VisibleForTesting
    public static List<byte[]> getDecryptionKeyMetadata(int currentStripe, List<StripeInformation> stripes)
    {
        // if this stripe has encryption keys, then those are used
        // otherwise look at nearest prior stripe that specifies encryption keys
        // if the first stripe has no encryption information, then there are no encrypted columns;
        for (int i = currentStripe; i >= 0; i--) {
            if (!stripes.get(i).getKeyMetadata().isEmpty()) {
                return stripes.get(i).getKeyMetadata();
            }
        }

        return ImmutableList.of();
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.apply(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(long rowCount)
    {
        if (writeChecksumBuilder.isPresent()) {
            writeChecksumBuilder.get().addStripe(rowCount);
        }
    }

    private static Map<Integer, ColumnStatistics> getStatisticsByColumnOrdinal(OrcType rootStructType, List<ColumnStatistics> fileStats)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == STRUCT);
        requireNonNull(fileStats, "fileStats is null");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            if (fileStats.size() > ordinal) {
                ColumnStatistics element = fileStats.get(rootStructType.getFieldTypeIndex(ordinal));
                if (element != null) {
                    statistics.put(ordinal, element);
                }
            }
        }
        return statistics.build();
    }

    /**
     * @return The size of memory retained by all the stream readers (local buffers + object overhead)
     */
    @VisibleForTesting
    long getStreamReaderRetainedSizeInBytes()
    {
        long totalRetainedSizeInBytes = 0;
        for (StreamReader column : streamReaders) {
            if (column != null) {
                totalRetainedSizeInBytes += column.getRetainedSizeInBytes();
            }
        }
        return totalRetainedSizeInBytes;
    }

    /**
     * @return The size of memory retained by the current stripe (excludes object overheads)
     */
    @VisibleForTesting
    long getCurrentStripeRetainedSizeInBytes()
    {
        return currentStripeSystemMemoryContext.getBytes();
    }

    protected long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + getStreamReaderRetainedSizeInBytes() + getCurrentStripeRetainedSizeInBytes();
    }

    /**
     * @return The system memory reserved by this OrcRecordReader. It does not include non-leaf level StreamReaders'
     * instance sizes.
     */
    @VisibleForTesting
    long getSystemMemoryUsage()
    {
        return systemMemoryUsage.getBytes();
    }

    protected static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<OrcType> types, OrcDataSource dataSource)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getOrcTypeKind() == STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getOrcTypeKind() == OrcType.OrcTypeKind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getOrcTypeKind() == OrcType.OrcTypeKind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type, dataSource, nestedStreams.build());
    }

    protected boolean shouldValidateWritePageChecksum()
    {
        return writeChecksumBuilder.isPresent();
    }

    protected void validateWritePageChecksum(Page page)
    {
        if (writeChecksumBuilder.isPresent()) {
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.get().addPage(page);
            stripeStatisticsValidation.get().addPage(page);
            fileStatisticsValidation.get().addPage(page);
        }
    }

    private static class StripeInfo
    {
        private final StripeInformation stripe;
        private final Optional<StripeStatistics> stats;

        public StripeInfo(StripeInformation stripe, Optional<StripeStatistics> stats)
        {
            this.stripe = requireNonNull(stripe, "stripe is null");
            this.stats = requireNonNull(stats, "metadata is null");
        }

        public StripeInformation getStripe()
        {
            return stripe;
        }

        public Optional<StripeStatistics> getStats()
        {
            return stats;
        }
    }

    @VisibleForTesting
    static class LinearProbeRangeFinder
            implements CachingOrcDataSource.RegionFinder
    {
        private final List<DiskRange> diskRanges;
        private int index;

        public LinearProbeRangeFinder(List<DiskRange> diskRanges)
        {
            this.diskRanges = diskRanges;
        }

        @Override
        public DiskRange getRangeFor(long desiredOffset)
        {
            // Assumption: range are always read in order
            // Assumption: bytes that are not part of any range are never read
            for (; index < diskRanges.size(); index++) {
                DiskRange range = diskRanges.get(index);
                if (range.getEnd() > desiredOffset) {
                    checkArgument(range.getOffset() <= desiredOffset);
                    return range;
                }
            }
            throw new IllegalArgumentException("Invalid desiredOffset " + desiredOffset);
        }

        public static OrcBatchRecordReader.LinearProbeRangeFinder createTinyStripesRangeFinder(List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
        {
            if (stripes.size() == 0) {
                return new OrcBatchRecordReader.LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> diskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, tinyStripeThreshold);

            return new OrcBatchRecordReader.LinearProbeRangeFinder(diskRanges);
        }
    }
}
