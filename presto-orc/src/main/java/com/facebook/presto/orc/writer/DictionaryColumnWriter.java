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
import com.facebook.presto.orc.stream.LongOutputStreamV1;
import com.facebook.presto.orc.stream.LongOutputStreamV2;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public abstract class DictionaryColumnWriter
        implements ColumnWriter, DictionaryColumn
{
    protected final int column;
    protected final Type type;
    protected final ColumnWriterOptions columnWriterOptions;
    protected final Optional<DwrfDataEncryptor> dwrfEncryptor;
    protected final OrcEncoding orcEncoding;
    protected final MetadataWriter metadataWriter;

    private final LongOutputStream dataStream;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter compressedMetadataWriter;
    private final List<DictionaryRowGroup> rowGroups = new ArrayList<>();
    private final int preserveDirectEncodingStripeCount;

    private int[] rowGroupIndexes;
    private int rowGroupValueCount;
    private long rawBytes;
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
        this.type = requireNonNull(type, "type is null");
        this.columnWriterOptions = requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        this.dwrfEncryptor = requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");
        LongOutputStream result;
        if (orcEncoding == DWRF) {
            result = new LongOutputStreamV1(columnWriterOptions, dwrfEncryptor, false, DATA);
        }
        else {
            result = new LongOutputStreamV2(columnWriterOptions, false, DATA);
        }
        this.dataStream = result;
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.compressedMetadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.rowGroupIndexes = new int[10_000];
        this.preserveDirectEncodingStripeCount = columnWriterOptions.getPreserveDirectEncodingStripeCount();
    }

    protected abstract ColumnWriter createDirectColumnWriter();

    protected abstract ColumnWriter getDirectColumnWriter();

    protected abstract boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, int[] dictionaryIndexes, int maxDirectBytes);

    protected abstract ColumnEncoding getDictionaryColumnEncoding();

    protected abstract BlockStatistics addBlockToDictionary(Block block, int rowGroupValueCount, int[] rowGroupIndexes);

    protected abstract long getRetainedDictionaryBytes();

    /**
     * writeDictionary to the Streams and optionally return new mappings to be used.
     * The mapping is used for sorting the indexes. ORC dictionary needs to be sorted,
     * but DWRF sorting is optional.
     * @return new mappings to be used for indexes, if no new mappings, Optional.empty.
     */
    protected abstract Optional<int[]> writeDictionary();

    protected abstract void writePresentAndDataStreams(
            int rowGroupValueCount,
            int[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            PresentOutputStream presentStream,
            LongOutputStream dataStream);

    protected abstract void resetDictionary();

    protected abstract void closeDictionary();

    protected abstract List<StreamDataOutput> getDictionaryStreams(int column);

    protected abstract ColumnStatistics createColumnStatistics();

    @Override
    public long getRawBytes()
    {
        checkState(!directEncoded);
        return rawBytes;
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
    public OptionalInt tryConvertToDirect(int maxDirectBytes)
    {
        checkState(!closed);
        checkState(!directEncoded);
        ColumnWriter directWriter = createDirectColumnWriter();
        checkState(directWriter.getBufferedBytes() == 0);

        for (DictionaryRowGroup rowGroup : rowGroups) {
            directWriter.beginRowGroup();
            // todo we should be able to pass the stats down to avoid recalculating min and max
            boolean success = tryConvertRowGroupToDirect(rowGroup.getValueCount(), rowGroup.getDictionaryIndexes(), maxDirectBytes);
            directWriter.finishRowGroup();

            if (!success) {
                directWriter.close();
                directWriter.reset();
                return OptionalInt.empty();
            }
        }

        if (inRowGroup) {
            directWriter.beginRowGroup();
            if (!tryConvertRowGroupToDirect(rowGroupValueCount, rowGroupIndexes, maxDirectBytes)) {
                directWriter.close();
                directWriter.reset();
                return OptionalInt.empty();
            }
        }
        else {
            checkState(rowGroupValueCount == 0);
        }

        rowGroups.clear();

        // free the dictionary

        rawBytes = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        rowGroupValueCount = 0;
        closeDictionary();
        resetDictionary();
        directEncoded = true;

        return OptionalInt.of(toIntExact(directWriter.getBufferedBytes()));
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
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        if (directEncoded) {
            return getDirectColumnWriter().writeBlock(block);
        }

        rowGroupIndexes = ensureCapacity(rowGroupIndexes, rowGroupValueCount + block.getPositionCount(), MEDIUM, PRESERVE);
        BlockStatistics blockStatistics = addBlockToDictionary(block, rowGroupValueCount, rowGroupIndexes);
        totalNonNullValueCount += blockStatistics.getNonNullValueCount();
        rawBytes += blockStatistics.getRawBytes();
        rowGroupValueCount += block.getPositionCount();
        totalValueCount += block.getPositionCount();
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
        DictionaryRowGroup rowGroup = new DictionaryRowGroup(rowGroupIndexes, rowGroupValueCount, statistics);
        rowGroups.add(rowGroup);
        if (columnWriterOptions.isIgnoreDictionaryRowGroupSizes()) {
            rowGroupRetainedSizeInBytes += rowGroup.getColumnStatistics().getRetainedSizeInBytes();
        }
        else {
            rowGroupRetainedSizeInBytes += rowGroup.getRetainedSizeInBytes();
        }
        rowGroupValueCount = 0;
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
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            writePresentAndDataStreams(
                    rowGroup.getValueCount(),
                    rowGroup.getDictionaryIndexes(),
                    originalDictionaryToSortedIndex,
                    presentStream,
                    dataStream);
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }

        closeDictionary();
        dataStream.close();
        presentStream.close();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return getDirectColumnWriter().getIndexStreams();
        }

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();
        List<LongStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroups.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroups.get(groupId).getColumnStatistics();
            LongStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(columnWriterOptions.getCompressionKind() != NONE, dataCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = compressedMetadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(column, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
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
        return getIndexBytes() + getDictionaryBytes();
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
                dataStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                getRetainedDictionaryBytes() +
                rowGroupRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        dataStream.reset();
        presentStream.reset();
        rowGroups.clear();
        rowGroupRetainedSizeInBytes = 0;
        rowGroupValueCount = 0;
        resetDictionary();

        rawBytes = 0;
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

    private static class DictionaryRowGroup
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryRowGroup.class).instanceSize();

        private final int[] dictionaryIndexes;
        private final ColumnStatistics columnStatistics;

        public DictionaryRowGroup(int[] dictionaryIndexes, int valueCount, ColumnStatistics columnStatistics)
        {
            requireNonNull(dictionaryIndexes, "dictionaryIndexes is null");
            checkArgument(valueCount >= 0, "valueCount is negative");
            requireNonNull(columnStatistics, "columnStatistics is null");

            this.dictionaryIndexes = Arrays.copyOf(dictionaryIndexes, valueCount);
            this.columnStatistics = columnStatistics;
        }

        public int[] getDictionaryIndexes()
        {
            return dictionaryIndexes;
        }

        public int getValueCount()
        {
            return dictionaryIndexes.length;
        }

        public ColumnStatistics getColumnStatistics()
        {
            return columnStatistics;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE +
                    sizeOf(dictionaryIndexes) +
                    columnStatistics.getRetainedSizeInBytes();
        }
    }

    static class BlockStatistics
    {
        private final int nonNullValueCount;
        private final long rawBytes;
        private final long rawBytesIncludingNulls;

        public BlockStatistics(int nonNullValueCount, long rawBytes, long rawBytesIncludingNulls)
        {
            this.nonNullValueCount = nonNullValueCount;
            this.rawBytes = rawBytes;
            this.rawBytesIncludingNulls = rawBytesIncludingNulls;
        }

        public int getNonNullValueCount()
        {
            return nonNullValueCount;
        }

        public long getRawBytes()
        {
            return rawBytes;
        }

        public long getRawBytesIncludingNulls()
        {
            return rawBytesIncludingNulls;
        }
    }
}
