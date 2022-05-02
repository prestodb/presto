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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DictionaryCompressionOptimizer.DictionaryColumn;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.stream.LongOutputStream.createDataOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public abstract class DictionaryColumnWriter
        implements ColumnWriter, DictionaryColumn
{
    // In theory, nulls are stored using bit fields with 1 bit per entry
    // In code, though they use Byte RLE, using 8 is a good heuristic and close to worst case.
    public static final int NUMBER_OF_NULLS_PER_BYTE = 8;
    private static final int EXPECTED_ROW_GROUP_SEGMENT_SIZE = 10_000;

    protected final int column;
    protected final ColumnWriterOptions columnWriterOptions;
    protected final Optional<DwrfDataEncryptor> dwrfEncryptor;
    protected final OrcEncoding orcEncoding;
    protected final MetadataWriter metadataWriter;

    private final CompressedMetadataWriter compressedMetadataWriter;
    private final List<DictionaryRowGroup> rowGroups = new ArrayList<>();
    private final DictionaryRowGroupBuilder rowGroupBuilder = new DictionaryRowGroupBuilder();
    private final int preserveDirectEncodingStripeCount;

    private PresentOutputStream presentStream;
    private LongOutputStream dataStream;
    private int[] rowGroupIndexes;
    private int rowGroupOffset;
    private long rawBytesEstimate;
    private long totalValueCount;
    private long totalNonNullValueCount;
    private boolean closed;
    private boolean inRowGroup;
    private boolean directEncoded;
    private long rowGroupRetainedSizeInBytes;
    private int preserveDirectEncodingStripeIndex;

    public DictionaryColumnWriter(
            int column,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        this.column = column;
        this.columnWriterOptions = requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        this.dwrfEncryptor = requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");
        this.compressedMetadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.preserveDirectEncodingStripeCount = columnWriterOptions.getPreserveDirectEncodingStripeCount();

        this.dataStream = createDataOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.rowGroupIndexes = new int[EXPECTED_ROW_GROUP_SEGMENT_SIZE];
    }

    protected abstract ColumnWriter createDirectColumnWriter();

    protected abstract ColumnWriter getDirectColumnWriter();

    protected abstract boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, int[] dictionaryIndexes, int maxDirectBytes);

    protected abstract boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, short[] dictionaryIndexes, int maxDirectBytes);

    protected abstract boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, byte[] dictionaryIndexes, int maxDirectBytes);

    protected abstract ColumnEncoding getDictionaryColumnEncoding();

    protected abstract BlockStatistics addBlockToDictionary(Block block, int rowGroupOffset, int[] rowGroupIndexes);

    protected abstract long getRetainedDictionaryBytes();

    /**
     * writeDictionary to the Streams and optionally return new mappings to be used.
     * The mapping is used for sorting the indexes. ORC dictionary needs to be sorted,
     * but DWRF sorting is optional.
     *
     * @return new mappings to be used for indexes, if no new mappings, Optional.empty.
     */
    protected abstract Optional<int[]> writeDictionary();

    protected abstract void beginDataRowGroup();

    protected abstract void movePresentStreamToDirectWriter(PresentOutputStream presentStream);

    protected abstract void writeDataStreams(
            int rowGroupValueCount,
            byte[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream);

    protected abstract void writeDataStreams(
            int rowGroupValueCount,
            short[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream);

    protected abstract void writeDataStreams(
            int rowGroupValueCount,
            int[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream);

    protected abstract void resetDictionary();

    protected abstract void closeDictionary();

    protected abstract List<StreamDataOutput> getDictionaryStreams(int column);

    protected abstract ColumnStatistics createColumnStatistics();

    @Override
    public long getRawBytesEstimate()
    {
        checkState(!directEncoded);
        return rawBytesEstimate;
    }

    @Override
    public boolean isDirectEncoded()
    {
        return directEncoded;
    }

    @Override
    public int getIndexBytes()
    {
        checkState(!directEncoded);
        return toIntExact(estimateIndexBytesPerValue(getDictionaryEntries()) * getNonNullValueCount());
    }

    @Override
    public long getValueCount()
    {
        checkState(!directEncoded);
        return totalValueCount;
    }

    @Override
    public long getNonNullValueCount()
    {
        checkState(!directEncoded);
        return totalNonNullValueCount;
    }

    @Override
    public long getNullValueCount()
    {
        checkState(!directEncoded);
        return totalValueCount - totalNonNullValueCount;
    }

    private boolean tryConvertRowGroupToDirect(byte[][] byteSegments, short[][] shortSegments, int[][] intSegments, int maxDirectBytes)
    {
        // The row group indexes may be split between byte, short and int segments. They need to be processed in
        // byte, short and int order. If they are processed in different order, it will result in data corruption.
        if (byteSegments != null) {
            for (byte[] byteIndexes : byteSegments) {
                if (!tryConvertRowGroupToDirect(byteIndexes.length, byteIndexes, maxDirectBytes)) {
                    return false;
                }
            }
        }

        if (shortSegments != null) {
            for (short[] shortIndexes : shortSegments) {
                if (!tryConvertRowGroupToDirect(shortIndexes.length, shortIndexes, maxDirectBytes)) {
                    return false;
                }
            }
        }

        if (intSegments != null) {
            for (int[] intIndexes : intSegments) {
                if (!tryConvertRowGroupToDirect(intIndexes.length, intIndexes, maxDirectBytes)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public OptionalInt tryConvertToDirect(int maxDirectBytes)
    {
        checkState(!closed);
        checkState(!directEncoded);
        ColumnWriter directWriter = createDirectColumnWriter();
        checkState(directWriter.getBufferedBytes() == 0, "direct writer should have no data");

        for (DictionaryRowGroup rowGroup : rowGroups) {
            beginDataRowGroup();
            // todo we should be able to pass the stats down to avoid recalculating min and max
            boolean success = tryConvertRowGroupToDirect(rowGroup.getByteSegments(), rowGroup.getShortSegments(), rowGroup.getIntSegments(), maxDirectBytes);

            if (!success) {
                return resetDirectWriter(directWriter);
            }
            directWriter.finishRowGroup();
        }

        if (inRowGroup) {
            beginDataRowGroup();
            boolean success = tryConvertRowGroupToDirect(
                    rowGroupBuilder.getByteSegments(),
                    rowGroupBuilder.getShortSegments(),
                    rowGroupBuilder.getIntegerSegments(),
                    maxDirectBytes);

            if (!success) {
                return resetDirectWriter(directWriter);
            }

            if (!tryConvertRowGroupToDirect(rowGroupOffset, rowGroupIndexes, maxDirectBytes)) {
                return resetDirectWriter(directWriter);
            }
        }
        else {
            checkState(rowGroupOffset == 0);
        }

        // Conversion to DirectStream succeeded, Transfer the present stream to direct writer and assign
        // this a new PresentStream, so one writer is responsible for one present stream.
        movePresentStreamToDirectWriter(presentStream);
        presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);

        // free the dictionary
        rawBytesEstimate = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        resetRowGroups();
        closeDictionary();
        resetDictionary();
        directEncoded = true;

        return OptionalInt.of(toIntExact(directWriter.getBufferedBytes()));
    }

    private OptionalInt resetDirectWriter(ColumnWriter directWriter)
    {
        directWriter.close();
        directWriter.reset();
        return OptionalInt.empty();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        checkState(closed);
        if (directEncoded) {
            return getDirectColumnWriter().getColumnEncodings();
        }
        return ImmutableMap.of(column, getDictionaryColumnEncoding());
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!inRowGroup);
        inRowGroup = true;

        if (directEncoded) {
            getDirectColumnWriter().beginRowGroup();
        }
        else {
            presentStream.recordCheckpoint();
        }
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        if (directEncoded) {
            return getDirectColumnWriter().writeBlock(block);
        }

        rowGroupIndexes = ensureCapacity(rowGroupIndexes, rowGroupOffset + block.getPositionCount(), MEDIUM, PRESERVE);

        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }
        BlockStatistics blockStatistics = addBlockToDictionary(block, rowGroupOffset, rowGroupIndexes);
        totalNonNullValueCount += blockStatistics.getNonNullValueCount();
        rawBytesEstimate += blockStatistics.getRawBytesEstimate();
        rowGroupOffset += blockStatistics.getNonNullValueCount();
        totalValueCount += block.getPositionCount();
        if (rowGroupOffset >= EXPECTED_ROW_GROUP_SEGMENT_SIZE) {
            rowGroupBuilder.addIndexes(getDictionaryEntries() - 1, rowGroupIndexes, rowGroupOffset);
            rowGroupOffset = 0;
        }
        return blockStatistics.getRawBytesIncludingNulls();
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        checkState(inRowGroup);
        inRowGroup = false;

        if (directEncoded) {
            return getDirectColumnWriter().finishRowGroup();
        }

        ColumnStatistics statistics = createColumnStatistics();
        rowGroupBuilder.addIndexes(getDictionaryEntries() - 1, rowGroupIndexes, rowGroupOffset);
        DictionaryRowGroup rowGroup = rowGroupBuilder.build(statistics);
        rowGroups.add(rowGroup);
        if (columnWriterOptions.isIgnoreDictionaryRowGroupSizes()) {
            rowGroupRetainedSizeInBytes += rowGroup.getColumnStatistics().getRetainedSizeInBytes();
        }
        else {
            rowGroupRetainedSizeInBytes += rowGroup.getShallowRetainedSizeInBytes();
            rowGroupRetainedSizeInBytes += rowGroupBuilder.getIndexRetainedBytes();
        }
        rowGroupOffset = 0;
        rowGroupBuilder.reset();
        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        checkState(!inRowGroup);
        closed = true;
        if (directEncoded) {
            getDirectColumnWriter().close();
        }
        else {
            bufferOutputData();
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        if (directEncoded) {
            return getDirectColumnWriter().getColumnStripeStatistics();
        }

        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroups.stream()
                .map(DictionaryRowGroup::getColumnStatistics)
                .collect(toList())));
    }

    private void bufferOutputData()
    {
        checkState(closed);
        checkState(!directEncoded);

        Optional<int[]> originalDictionaryToSortedIndex = writeDictionary();
        if (!rowGroups.isEmpty()) {
            dataStream.recordCheckpoint();
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            // The row group indexes may be split between byte, short and int segments. They need to be processed in
            // byte, short and int order. If they are processed in different order, it will result in data corruption.
            byte[][] byteSegments = rowGroup.getByteSegments();
            if (byteSegments != null) {
                for (byte[] byteIndexes : byteSegments) {
                    writeDataStreams(
                            byteIndexes.length,
                            byteIndexes,
                            originalDictionaryToSortedIndex,
                            dataStream);
                }
            }

            short[][] shortSegments = rowGroup.getShortSegments();
            if (shortSegments != null) {
                for (short[] shortIndexes : shortSegments) {
                    writeDataStreams(
                            shortIndexes.length,
                            shortIndexes,
                            originalDictionaryToSortedIndex,
                            dataStream);
                }
            }

            int[][] intSegments = rowGroup.getIntSegments();
            if (intSegments != null) {
                for (int[] integerIndexes : intSegments) {
                    writeDataStreams(
                            integerIndexes.length,
                            integerIndexes,
                            originalDictionaryToSortedIndex,
                            dataStream);
                }
            }

            dataStream.recordCheckpoint();
        }

        closeDictionary();
        dataStream.close();
        presentStream.close();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<BooleanStreamCheckpoint>> dwrfFlatMapStreamCheckpoints)
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return getDirectColumnWriter().getIndexStreams(dwrfFlatMapStreamCheckpoints);
        }

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();
        List<LongStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroups.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroups.get(groupId).getColumnStatistics();
            LongStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            Optional<BooleanStreamCheckpoint> inMapCheckpoint = dwrfFlatMapStreamCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(columnWriterOptions.getCompressionKind() != NONE, dataCheckpoint, presentCheckpoint, inMapCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = compressedMetadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint,
            Optional<BooleanStreamCheckpoint> inMapCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        inMapCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        if (directEncoded) {
            return getDirectColumnWriter().getDataStreams();
        }

        // actually write data
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column).ifPresent(outputDataStreams::add);
        outputDataStreams.add(dataStream.getStreamDataOutput(column));
        outputDataStreams.addAll(getDictionaryStreams(column));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        checkState(!closed);
        if (directEncoded) {
            return getDirectColumnWriter().getBufferedBytes();
        }
        // for dictionary columns we report the data we expect to write to the output stream
        long numberOfNullBytes = getNullValueCount() / NUMBER_OF_NULLS_PER_BYTE;
        return getIndexBytes() + getDictionaryBytes() + numberOfNullBytes;
    }

    @VisibleForTesting
    public long getRowGroupRetainedSizeInBytes()
    {
        return rowGroupRetainedSizeInBytes;
    }

    @Override
    public long getRetainedBytes()
    {
        return sizeOf(rowGroupIndexes) +
                rowGroupBuilder.getRetainedSizeInBytes() +
                dataStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                getRetainedDictionaryBytes() +
                rowGroupRetainedSizeInBytes;
    }

    private void resetRowGroups()
    {
        rowGroups.clear();
        rowGroupBuilder.reset();
        rowGroupRetainedSizeInBytes = 0;
        rowGroupOffset = 0;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        presentStream.reset();
        // Dictionary data is held in memory, until the Stripe is flushed. OrcOutputStream maintains the
        // allocated buffer and reuses the buffer for writing data. For Direct writer, OrcOutputStream
        // behavior avoids the reallocation of the buffers by maintaining the pool. For Dictionary writer,
        // OrcOutputStream doubles the memory requirement in most cases (one for dictionary and one for
        // OrcOutputBuffer). To avoid this, the streams are reallocated for every stripe.
        dataStream = createDataOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        resetDictionary();
        resetRowGroups();
        rawBytesEstimate = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        if (directEncoded) {
            getDirectColumnWriter().reset();
            if (preserveDirectEncodingStripeIndex >= preserveDirectEncodingStripeCount) {
                directEncoded = false;
                preserveDirectEncodingStripeIndex = 0;
            }
            else {
                preserveDirectEncodingStripeIndex++;
            }
        }
    }

    static class BlockStatistics
    {
        private final int nonNullValueCount;
        private final long rawBytesEstimate;
        private final long rawBytesIncludingNulls;

        public BlockStatistics(int nonNullValueCount, long rawBytesEstimate, long rawBytesIncludingNulls)
        {
            this.nonNullValueCount = nonNullValueCount;
            this.rawBytesEstimate = rawBytesEstimate;
            this.rawBytesIncludingNulls = rawBytesIncludingNulls;
        }

        public int getNonNullValueCount()
        {
            return nonNullValueCount;
        }

        public long getRawBytesEstimate()
        {
            return rawBytesEstimate;
        }

        public long getRawBytesIncludingNulls()
        {
            return rawBytesIncludingNulls;
        }
    }
}
