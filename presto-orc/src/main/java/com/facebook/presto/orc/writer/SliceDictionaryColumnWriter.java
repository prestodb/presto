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
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DictionaryCompressionOptimizer.DictionaryColumn;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.array.IntBigArray;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.stream.ByteArrayOutputStream;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.LongOutputStreamV1;
import com.facebook.presto.orc.stream.LongOutputStreamV2;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SliceDictionaryColumnWriter
        implements ColumnWriter, DictionaryColumn
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnWriter.class).instanceSize();
    private static final int DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES = toIntExact(new DataSize(32, MEGABYTE).toBytes());

    private final int column;
    private final Type type;
    private final CompressionParameters compressionParameters;
    private final Optional<DwrfDataEncryptor> dwrfEncryptor;
    private final OrcEncoding orcEncoding;
    private final int stringStatisticsLimitInBytes;

    private final LongOutputStream dataStream;
    private final PresentOutputStream presentStream;
    private final ByteArrayOutputStream dictionaryDataStream;
    private final LongOutputStream dictionaryLengthStream;
    private final CompressedMetadataWriter compressedMetadataWriter;
    private final MetadataWriter metadataWriter;

    private final DictionaryBuilder dictionary = new DictionaryBuilder(10000);

    private final List<DictionaryRowGroup> rowGroups = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private IntBigArray values;
    private int rowGroupValueCount;
    private StringStatisticsBuilder statisticsBuilder;

    private long rawBytes;
    private long totalValueCount;
    private long totalNonNullValueCount;

    private boolean closed;
    private boolean inRowGroup;
    private ColumnEncoding columnEncoding;

    private boolean directEncoded;
    private SliceDirectColumnWriter directColumnWriter;

    public SliceDictionaryColumnWriter(
            int column,
            Type type,
            CompressionParameters compressionParameters,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            DataSize stringStatisticsLimit,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        this.column = column;
        this.type = requireNonNull(type, "type is null");
        this.compressionParameters = requireNonNull(compressionParameters, "compressionParameters is null");
        this.dwrfEncryptor = requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        this.orcEncoding = requireNonNull(orcEncoding, "orcEncoding is null");
        this.stringStatisticsLimitInBytes = toIntExact(requireNonNull(stringStatisticsLimit, "stringStatisticsLimit is null").toBytes());
        LongOutputStream result;
        if (orcEncoding == DWRF) {
            result = new LongOutputStreamV1(compressionParameters, dwrfEncryptor, false, DATA);
        }
        else {
            result = new LongOutputStreamV2(compressionParameters, false, DATA);
        }
        this.dataStream = result;
        this.presentStream = new PresentOutputStream(compressionParameters, dwrfEncryptor);
        this.dictionaryDataStream = new ByteArrayOutputStream(compressionParameters, dwrfEncryptor, StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(compressionParameters, dwrfEncryptor, orcEncoding);
        this.metadataWriter = requireNonNull(metadataWriter, "metadataWriter is null");
        this.compressedMetadataWriter = new CompressedMetadataWriter(metadataWriter, compressionParameters, dwrfEncryptor);
        values = new IntBigArray();
        this.statisticsBuilder = newStringStatisticsBuilder();
    }

    @Override
    public long getRawBytes()
    {
        checkState(!directEncoded);
        return rawBytes;
    }

    @Override
    public int getDictionaryBytes()
    {
        checkState(!directEncoded);
        return toIntExact(dictionary.getSizeInBytes());
    }

    @Override
    public int getIndexBytes()
    {
        checkState(!directEncoded);
        return toIntExact(estimateIndexBytesPerValue(dictionary.getEntryCount()) * getNonNullValueCount());
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
    public int getDictionaryEntries()
    {
        checkState(!directEncoded);
        return dictionary.getEntryCount();
    }

    @Override
    public OptionalInt tryConvertToDirect(int maxDirectBytes)
    {
        checkState(!closed);
        checkState(!directEncoded);
        if (directColumnWriter == null) {
            directColumnWriter = new SliceDirectColumnWriter(column, type, compressionParameters, dwrfEncryptor, orcEncoding, this::newStringStatisticsBuilder, metadataWriter);
        }
        checkState(directColumnWriter.getBufferedBytes() == 0);

        Block dictionaryValues = dictionary.getElementBlock();
        for (DictionaryRowGroup rowGroup : rowGroups) {
            directColumnWriter.beginRowGroup();
            // todo we should be able to pass the stats down to avoid recalculating min and max
            boolean success = writeDictionaryRowGroup(dictionaryValues, rowGroup.getValueCount(), rowGroup.getDictionaryIndexes(), maxDirectBytes);
            directColumnWriter.finishRowGroup();

            if (!success) {
                directColumnWriter.close();
                directColumnWriter.reset();
                return OptionalInt.empty();
            }
        }

        if (inRowGroup) {
            directColumnWriter.beginRowGroup();
            if (!writeDictionaryRowGroup(dictionaryValues, rowGroupValueCount, values, maxDirectBytes)) {
                directColumnWriter.close();
                directColumnWriter.reset();
                return OptionalInt.empty();
            }
        }
        else {
            checkState(rowGroupValueCount == 0);
        }

        rowGroups.clear();

        // free the dictionary
        dictionary.clear();

        rawBytes = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        rowGroupValueCount = 0;
        statisticsBuilder = newStringStatisticsBuilder();

        directEncoded = true;

        return OptionalInt.of(toIntExact(directColumnWriter.getBufferedBytes()));
    }

    private boolean writeDictionaryRowGroup(Block dictionary, int valueCount, IntBigArray dictionaryIndexes, int maxDirectBytes)
    {
        int[][] segments = dictionaryIndexes.getSegments();
        for (int i = 0; valueCount > 0 && i < segments.length; i++) {
            int[] segment = segments[i];
            int positionCount = Math.min(valueCount, segment.length);
            Block block = new DictionaryBlock(positionCount, dictionary, segment);

            while (block != null) {
                int chunkPositionCount = block.getPositionCount();
                Block chunk = block.getRegion(0, chunkPositionCount);

                // avoid chunk with huge logical size
                while (chunkPositionCount > 1 && chunk.getLogicalSizeInBytes() > DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
                    chunkPositionCount /= 2;
                    chunk = chunk.getRegion(0, chunkPositionCount);
                }

                directColumnWriter.writeBlock(chunk);
                if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                    return false;
                }

                // slice block to only unconverted rows
                if (chunkPositionCount < block.getPositionCount()) {
                    block = block.getRegion(chunkPositionCount, block.getPositionCount() - chunkPositionCount);
                }
                else {
                    block = null;
                }
            }

            valueCount -= positionCount;
        }
        checkState(valueCount == 0);
        return true;
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        checkState(closed);
        if (directEncoded) {
            return directColumnWriter.getColumnEncodings();
        }
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!inRowGroup);
        inRowGroup = true;

        if (directEncoded) {
            directColumnWriter.beginRowGroup();
        }
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        if (directEncoded) {
            directColumnWriter.writeBlock(block);
            return;
        }

        // record values
        values.ensureCapacity(rowGroupValueCount + block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            int index = dictionary.putIfAbsent(block, position);
            values.set(rowGroupValueCount, index);
            rowGroupValueCount++;
            totalValueCount++;

            if (!block.isNull(position)) {
                // todo min/max statistics only need to be updated if value was not already in the dictionary, but non-null count does
                statisticsBuilder.addValue(type.getSlice(block, position));

                rawBytes += block.getSliceLength(position);
                totalNonNullValueCount++;
            }
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        checkState(inRowGroup);
        inRowGroup = false;

        if (directEncoded) {
            return directColumnWriter.finishRowGroup();
        }

        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        DictionaryRowGroup rowGroup = new DictionaryRowGroup(values, rowGroupValueCount, statistics);
        rowGroups.add(rowGroup);
        columnStatisticsRetainedSizeInBytes += rowGroup.getColumnStatistics().getRetainedSizeInBytes();
        rowGroupValueCount = 0;
        statisticsBuilder = newStringStatisticsBuilder();
        values = new IntBigArray();
        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        checkState(!inRowGroup);
        closed = true;
        if (directEncoded) {
            directColumnWriter.close();
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
            return directColumnWriter.getColumnStripeStatistics();
        }

        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroups.stream()
                .map(DictionaryRowGroup::getColumnStatistics)
                .collect(toList())));
    }

    private void bufferOutputData()
    {
        checkState(closed);
        checkState(!directEncoded);

        Block dictionaryElements = dictionary.getElementBlock();

        // write dictionary in sorted order
        int[] sortedDictionaryIndexes = getSortedDictionaryNullsLast(dictionaryElements);
        for (int sortedDictionaryIndex : sortedDictionaryIndexes) {
            if (!dictionaryElements.isNull(sortedDictionaryIndex)) {
                int length = dictionaryElements.getSliceLength(sortedDictionaryIndex);
                dictionaryLengthStream.writeLong(length);
                Slice value = dictionaryElements.getSlice(sortedDictionaryIndex, 0, length);
                dictionaryDataStream.writeSlice(value);
            }
        }
        columnEncoding = new ColumnEncoding(orcEncoding == DWRF ? DICTIONARY : DICTIONARY_V2, dictionaryElements.getPositionCount() - 1);

        // build index from original dictionary index to new sorted position
        int[] originalDictionaryToSortedIndex = new int[sortedDictionaryIndexes.length];
        for (int sortOrdinal = 0; sortOrdinal < sortedDictionaryIndexes.length; sortOrdinal++) {
            int dictionaryIndex = sortedDictionaryIndexes[sortOrdinal];
            originalDictionaryToSortedIndex[dictionaryIndex] = sortOrdinal;
        }

        if (!rowGroups.isEmpty()) {
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            IntBigArray dictionaryIndexes = rowGroup.getDictionaryIndexes();
            for (int position = 0; position < rowGroup.getValueCount(); position++) {
                presentStream.writeBoolean(dictionaryIndexes.get(position) != 0);
            }
            for (int position = 0; position < rowGroup.getValueCount(); position++) {
                int originalDictionaryIndex = dictionaryIndexes.get(position);
                // index zero in original dictionary is reserved for null
                if (originalDictionaryIndex != 0) {
                    int sortedIndex = originalDictionaryToSortedIndex[originalDictionaryIndex];
                    if (sortedIndex < 0) {
                        throw new IllegalArgumentException();
                    }
                    dataStream.writeLong(sortedIndex);
                }
            }
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }

        // free the dictionary memory
        dictionary.clear();

        dictionaryDataStream.close();
        dictionaryLengthStream.close();

        dataStream.close();
        presentStream.close();
    }

    private static int[] getSortedDictionaryNullsLast(Block elementBlock)
    {
        int[] sortedPositions = new int[elementBlock.getPositionCount()];
        for (int i = 0; i < sortedPositions.length; i++) {
            sortedPositions[i] = i;
        }

        IntArrays.quickSort(sortedPositions, 0, sortedPositions.length, new AbstractIntComparator()
        {
            @Override
            public int compare(int left, int right)
            {
                boolean nullLeft = elementBlock.isNull(left);
                boolean nullRight = elementBlock.isNull(right);
                if (nullLeft && nullRight) {
                    return 0;
                }
                if (nullLeft) {
                    return 1;
                }
                if (nullRight) {
                    return -1;
                }

                return elementBlock.compareTo(
                        left,
                        0,
                        elementBlock.getSliceLength(left),
                        elementBlock,
                        right,
                        0,
                        elementBlock.getSliceLength(right));
            }
        });

        return sortedPositions;
    }

    @Override
    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return directColumnWriter.getIndexStreams();
        }

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroups.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroups.get(groupId).getColumnStatistics();
            LongStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(compressionParameters.getKind() != NONE, dataCheckpoint, presentCheckpoint);
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
            return directColumnWriter.getDataStreams();
        }

        // actually write data
        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column).ifPresent(outputDataStreams::add);
        outputDataStreams.add(dataStream.getStreamDataOutput(column));
        outputDataStreams.add(dictionaryLengthStream.getStreamDataOutput(column));
        outputDataStreams.add(dictionaryDataStream.getStreamDataOutput(column));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        checkState(!closed);
        if (directEncoded) {
            return directColumnWriter.getBufferedBytes();
        }
        // for dictionary columns we report the data we expect to write to the output stream
        return getIndexBytes() + getDictionaryBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE +
                values.sizeOf() +
                dataStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                dictionaryDataStream.getRetainedBytes() +
                dictionaryLengthStream.getRetainedBytes() +
                dictionary.getRetainedSizeInBytes() +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes()) +
                columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        dataStream.reset();
        presentStream.reset();
        dictionaryDataStream.reset();
        dictionaryLengthStream.reset();
        rowGroups.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        rowGroupValueCount = 0;
        statisticsBuilder = newStringStatisticsBuilder();
        columnEncoding = null;

        dictionary.clear();
        rawBytes = 0;
        totalValueCount = 0;
        totalNonNullValueCount = 0;

        if (directEncoded) {
            directEncoded = false;
            directColumnWriter.reset();
        }
    }

    private StringStatisticsBuilder newStringStatisticsBuilder()
    {
        return new StringStatisticsBuilder(stringStatisticsLimitInBytes);
    }

    private static class DictionaryRowGroup
    {
        private final IntBigArray dictionaryIndexes;
        private final int valueCount;
        private final ColumnStatistics columnStatistics;

        public DictionaryRowGroup(IntBigArray dictionaryIndexes, int valueCount, ColumnStatistics columnStatistics)
        {
            requireNonNull(dictionaryIndexes, "dictionaryIndexes is null");
            checkArgument(valueCount >= 0, "valueCount is negative");
            requireNonNull(columnStatistics, "columnStatistics is null");

            this.dictionaryIndexes = dictionaryIndexes;
            this.valueCount = valueCount;
            this.columnStatistics = columnStatistics;
        }

        public IntBigArray getDictionaryIndexes()
        {
            return dictionaryIndexes;
        }

        public int getValueCount()
        {
            return valueCount;
        }

        public ColumnStatistics getColumnStatistics()
        {
            return columnStatistics;
        }
    }
}
