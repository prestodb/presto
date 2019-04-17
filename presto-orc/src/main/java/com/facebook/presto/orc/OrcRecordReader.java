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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcWriteValidation.StatisticsValidation;
import com.facebook.presto.orc.OrcWriteValidation.WriteChecksum;
import com.facebook.presto.orc.OrcWriteValidation.WriteChecksumBuilder;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.reader.StreamReader;
import com.facebook.presto.orc.reader.StreamReaders;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSourceOptions;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.facebook.presto.orc.OrcReader.BATCH_SIZE_GROWTH_FACTOR;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.facebook.presto.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class OrcRecordReader
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcRecordReader.class).instanceSize();
    public static final long UNLIMITED_BUDGET = Long.MAX_VALUE;
    private static final int MIN_BATCH_ROWS = 4;
    private static final int ROW_GROUP_REVIEW_INTERVAL = 1;
    private static final int BATCH_HARD_SHRINK_FACTOR = 16;
    private static final int BATCH_SOFT_SHRINK_FACTOR = 16;
    private static final int BATCH_GROW_FACTOR = 2;

    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private final long maxBlockBytes;
    private final Map<Integer, Type> includedColumns;
    private long currentPosition;
    private long currentStripePosition;
    private int currentBatchSize;
    private int nextBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;
    private AggregatedMemoryContext currentStripeSystemMemoryContext;
    private AggregatedMemoryContext streamReadersSystemMemoryContext;

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private long filePosition;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private int currentRowGroup = -1;
    private RowGroup currentRowGroupObject;
    private int currentGroupRowCount;
    private long nextRowInGroup;

    private final Map<String, Slice> userMetadata;

    private final AggregatedMemoryContext systemMemoryUsage;

    private final Optional<OrcWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;
    private final Optional<StatisticsValidation> rowGroupStatisticsValidation;
    private final Optional<StatisticsValidation> stripeStatisticsValidation;
    private final Optional<StatisticsValidation> fileStatisticsValidation;

    private final boolean reorderFilters;
    private final boolean enforceMemoryBudget;

    private OrcPredicate predicate;
    // For getNextPage interface.
    private ColumnGroupReader reader;
    private QualifyingSet qualifyingSet;
    private int numResults;
    private int targetResultBytes;
    private int targetResultRows = 30000;
    private boolean reuseBlocks;
    private int ariaBatchRows = 1000;
    private long numAriaBytes;
    private long numAriaRows;

    public OrcRecordReader(
            Map<Integer, Type> includedColumns,
            Map<Integer, List<SubfieldPath>> includedSubfields,
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
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize tinyStripeThreshold,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            int initialBatchSize,
            boolean reorderFilters,
            boolean enforceMemoryBudget)
    {
        requireNonNull(includedColumns, "includedColumns is null");
        requireNonNull(includedSubfields, "includedSubfields is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(types, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");
        requireNonNull(systemMemoryUsage, "systemMemoryUsage is null");

        this.includedColumns = requireNonNull(includedColumns, "includedColumns is null");
        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(includedColumns));
        this.rowGroupStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.stripeStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.fileStatisticsValidation = writeValidation.map(validation -> validation.createWriteStatisticsBuilder(includedColumns));
        this.systemMemoryUsage = systemMemoryUsage.newAggregatedMemoryContext();
        this.predicate = predicate;
        this.reorderFilters = reorderFilters;
        this.enforceMemoryBudget = enforceMemoryBudget;

        // reduce the included columns to the set that is also present
        ImmutableSet.Builder<Integer> presentColumns = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, Type> presentColumnsAndTypes = ImmutableMap.builder();
        OrcType root = types.get(0);
        for (Map.Entry<Integer, Type> entry : includedColumns.entrySet()) {
            // an old file can have less columns since columns can be added
            // after the file was written
            if (entry.getKey() < root.getFieldCount()) {
                presentColumns.add(entry.getKey());
                presentColumnsAndTypes.put(entry.getKey(), entry.getValue());
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

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, maxMergeDistance, tinyStripeThreshold);
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.currentStripeSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();
        // The streamReadersSystemMemoryContext covers the StreamReader local buffer sizes, plus leaf node StreamReaders'
        // instance sizes who use local buffers. SliceDirectStreamReader's instance size is not counted, because it
        // doesn't have a local buffer. All non-leaf level StreamReaders' (e.g. MapStreamReader, LongStreamReader,
        // ListStreamReader and StructStreamReader) instance sizes were not counted, because calling setBytes() in
        // their constructors is confusing.
        this.streamReadersSystemMemoryContext = this.systemMemoryUsage.newAggregatedMemoryContext();

        stripeReader = new StripeReader(
                orcDataSource,
                decompressor,
                types,
                this.presentColumns,
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                writeValidation);

        streamReaders = createStreamReaders(orcDataSource, types, hiveStorageTimeZone, presentColumnsAndTypes.build(), includedSubfields, streamReadersSystemMemoryContext);
        maxBytesPerCell = new long[streamReaders.length];
        nextBatchSize = initialBatchSize;
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
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > tinyStripeThreshold.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, createTinyStripesRangeFinder(stripes, maxMergeDistance, tinyStripeThreshold));
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

    public float getProgress()
    {
        return ((float) currentPosition) / totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(orcDataSource);
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    closer.register(() -> column.close());
                }
            }
        }

        if (writeChecksumBuilder.isPresent()) {
            WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
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

    public int nextBatch()
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
        currentBatchSize = min(nextBatchSize, maxBatchSize);
        nextBatchSize = min(multiplyExact(currentBatchSize, BATCH_SIZE_GROWTH_FACTOR), MAX_BATCH_SIZE);
        currentBatchSize = min(currentBatchSize, toIntExact(currentGroupRowCount - nextRowInGroup));

        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.prepareNextRead(currentBatchSize);
            }
        }
        nextRowInGroup += currentBatchSize;

        validateWritePageChecksum();
        return currentBatchSize;
    }

    public Block readBlock(Type type, int columnIndex)
            throws IOException
    {
        Block block = streamReaders[columnIndex].readBlock(type);
        if (block.getPositionCount() > 0) {
            long bytesPerCell = block.getSizeInBytes() / block.getPositionCount();
            if (maxBytesPerCell[columnIndex] < bytesPerCell) {
                maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[columnIndex] + bytesPerCell;
                maxBytesPerCell[columnIndex] = bytesPerCell;
                maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / maxCombinedBytesPerRow)));
            }
        }
        return block;
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
                StatisticsValidation statisticsValidation = rowGroupStatisticsValidation.get();
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
        currentRowGroupObject = rowGroups.next();
        currentGroupRowCount = toIntExact(currentRowGroupObject.getRowCount());
        if (currentRowGroupObject.getMinAverageRowBytes() > 0) {
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / currentRowGroupObject.getMinAverageRowBytes())));
        }

        setReadersToCurrentRowGroup();
        return true;
    }

    private void setReadersToCurrentRowGroup()
            throws IOException
    {
        currentPosition = currentStripePosition + currentRowGroupObject.getRowOffset();
        filePosition = stripeFilePositions.get(currentStripe) + currentRowGroupObject.getRowOffset();

        // give reader data streams from row group
        InputStreamSources rowGroupStreamSources = currentRowGroupObject.getStreamSources();
        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeSystemMemoryContext.close();
        currentStripeSystemMemoryContext = systemMemoryUsage.newAggregatedMemoryContext();
        rowGroups = ImmutableList.<RowGroup>of().iterator();

        if (currentStripe >= 0) {
            if (stripeStatisticsValidation.isPresent()) {
                StatisticsValidation statisticsValidation = stripeStatisticsValidation.get();
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

        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeSystemMemoryContext, true);
        if (stripe != null) {
            // Give readers access to dictionary streams
            InputStreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            List<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    column.startStripe(dictionaryStreamSources, columnEncodings);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.apply(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(int rowCount)
    {
        if (writeChecksumBuilder.isPresent()) {
            writeChecksumBuilder.get().addStripe(rowCount);
        }
    }

    private void validateWritePageChecksum()
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            Block[] blocks = new Block[streamReaders.length];
            for (int columnIndex = 0; columnIndex < streamReaders.length; columnIndex++) {
                blocks[columnIndex] = readBlock(includedColumns.get(columnIndex), columnIndex);
            }
            Page page = new Page(currentBatchSize, blocks);
            writeChecksumBuilder.get().addPage(page);
            rowGroupStatisticsValidation.get().addPage(page);
            stripeStatisticsValidation.get().addPage(page);
            fileStatisticsValidation.get().addPage(page);
        }
    }

    private static StreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, Type> includedColumns,
            Map<Integer, List<SubfieldPath>> includedSubfields,
            AggregatedMemoryContext systemMemoryContext)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        StreamReader[] streamReaders = new StreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                streamReaders[columnId] = StreamReaders.createStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);

                List<SubfieldPath> subfieldPaths = includedSubfields.get(columnId);
                if (subfieldPaths != null) {
                    streamReaders[columnId].setReferencedSubfields(subfieldPaths, 0);
                }
            }
        }

        return streamReaders;
    }

    private static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<OrcType> types, OrcDataSource dataSource)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type.getOrcTypeKind(), dataSource, nestedStreams.build());
    }

    private static Map<Integer, ColumnStatistics> getStatisticsByColumnOrdinal(OrcType rootStructType, List<ColumnStatistics> fileStats)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
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

    /**
     * @return The total size of memory retained by this OrcRecordReader
     */
    @VisibleForTesting
    long getRetainedSizeInBytes()
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

        public static LinearProbeRangeFinder createTinyStripesRangeFinder(List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
        {
            if (stripes.size() == 0) {
                return new LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> diskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, tinyStripeThreshold);

            return new LinearProbeRangeFinder(diskRanges);
        }
    }

    public void pushdownFilterAndProjection(PageSourceOptions options, int[] channelColumns, List<Type> types)
    {
        reuseBlocks = options.getReusePages();
        reader = new ColumnGroupReader(
                streamReaders,
                presentColumns,
                channelColumns,
                types,
                options.getInternalChannels(),
                options.getOutputChannels(),
                predicate.getFilters(),
                options.getFilterFunctions(),
                enforceMemoryBudget,
                reorderFilters);
        targetResultBytes = options.getTargetBytes();
        reader.setResultSizeBudget(targetResultBytes);
    }

    public Page getNextPage()
            throws IOException
    {
        reader.newBatch(numResults);
        numResults = 0;
        for (; ; ) {
            if (currentRowGroup == -1 || qualifyingSet == null || (qualifyingSet.isEmpty() && qualifyingSet.getEnd() == currentGroupRowCount)) {
                if (currentPosition == totalRowCount) {
                    return null;
                }
                if (!advanceToNextRowGroup()) {
                    if (reorderFilters && currentRowGroup < ROW_GROUP_REVIEW_INTERVAL * 2) {
                        // If file ends before 2 reorder checks, do
                        // one more. Subsequent splits may start with
                        // the adaptation from the previous one.
                        reader.maybeReorderFilters();
                    }
                    filePosition = fileRowCount;
                    currentPosition = totalRowCount;
                    return resultPage();
                }
                if (qualifyingSet == null) {
                    qualifyingSet = new QualifyingSet();
                }
                qualifyingSet.setRange(0, Math.min(ariaBatchRows, currentGroupRowCount));
                reader.setQualifyingSets(qualifyingSet, null);
                if (currentRowGroup != 0 && (currentRowGroup % ROW_GROUP_REVIEW_INTERVAL == 0)) {
                    // Decay row size stats and reconsider filter
                    // order every ROW_GROUP_REVIEW_INTERVAL row
                    // groups.
                    numAriaRows /= 2;
                    numAriaBytes /= 2;
                    if (reorderFilters) {
                        reader.maybeReorderFilters();
                    }
                }
            }
            if (qualifyingSet.isEmpty()) {
                qualifyingSet.setRange(qualifyingSet.getEnd(), Math.min(qualifyingSet.getEnd() + ariaBatchRows, currentGroupRowCount));
            }
            long bytesBeforeAdvance = reader.getResultSizeInBytes();
            int numResultsBeforeAdvance = reader.getNumResults();
            try {
                reader.advance();
            }
            catch (BatchTooLargeException e) {
                // The reader ran out of budget. Retry with a smaller
                // input qualifying set. First remove any values added
                // by the last advance.  The input qualifying set is
                // unaltered when the reader throws this exception.
                // If we are at minimum batch size we must also have unlimited budget. Check that this path is not taken with minimum batch size.
                int batchSize = qualifyingSet.getEnd() - qualifyingSet.getPositions()[0];
                verify(batchSize > MIN_BATCH_ROWS, "Running out of unlimited budget with minimum batch size");
                reader.compactValues(new int[0], numResultsBeforeAdvance, 0);
                ariaBatchRows = Math.max(MIN_BATCH_ROWS, ariaBatchRows / BATCH_HARD_SHRINK_FACTOR);
                int[] rows = qualifyingSet.getPositions();
                int numRows = Math.min(ariaBatchRows, batchSize);
                qualifyingSet.setPositionCount(numRows);
                // The first row not in scope is 1 above the last in scope.
                qualifyingSet.setEnd(rows[numRows - 1] + 1);
                setReadersToCurrentRowGroup();
                if (reader.getNumResults() > 0) {
                    setReaderBudget();
                    return resultPage();
                }
                continue;
            }
            if (adjustAndCheckIfFullBatch(numResultsBeforeAdvance, bytesBeforeAdvance)) {
                return resultPage();
            }
        }
    }

    private boolean adjustAndCheckIfFullBatch(int numResultsBeforeAdvance, long bytesBeforeAdvance)
    {
        numResults = reader.getNumResults();
        long readerBytes = reader.getResultSizeInBytes();
        long bytesInLastBatch = readerBytes - bytesBeforeAdvance;
        numAriaBytes += bytesInLastBatch;
        numAriaRows += numResults - numResultsBeforeAdvance;
        if (bytesInLastBatch > targetResultBytes && ariaBatchRows > MIN_BATCH_ROWS) {
            // Make batch smaller if not already at minimum.
            ariaBatchRows = Math.max(ariaBatchRows / BATCH_SOFT_SHRINK_FACTOR, MIN_BATCH_ROWS);
        }
        else if (bytesInLastBatch == 0 && numAriaRows > 0) {
            // If skipping over no hits, do not make the batch
            // size too much larger than the number of hits that
            // fit in a batch so as to avoid a retry.
            long averageRowSize = numAriaBytes / (1 + numAriaRows);
            int averageNumHitsInBatch = toIntExact(targetResultBytes / (1 + averageRowSize));
            ariaBatchRows = Math.min(averageNumHitsInBatch * 2, ariaBatchRows * BATCH_SOFT_SHRINK_FACTOR);
        }
        else if (bytesInLastBatch < targetResultBytes / 8) {
            // If filled less than 1/8 of the quota, can have
            // larger batch next time.
            ariaBatchRows = Math.min(ariaBatchRows * BATCH_GROW_FACTOR, currentGroupRowCount);
        }
        setReaderBudget();
        // The result is full if there would not be room for another
        // batch of the same size as the last. The cap on row count
        // causes the scan to periodically return even if it does not
        // project out column values so that the caller can check for yield
        // and interruptions.
        return numResults > targetResultRows || readerBytes > targetResultBytes - bytesInLastBatch;
    }

    private void setReaderBudget()
    {
        reader.setResultSizeBudget(ariaBatchRows <= MIN_BATCH_ROWS ? UNLIMITED_BUDGET : targetResultBytes);
    }

    private Page resultPage()
    {
        if (numResults == 0) {
            return null;
        }
        return new Page(numResults, reader.getBlocks(numResults, reuseBlocks, false));
    }
}
