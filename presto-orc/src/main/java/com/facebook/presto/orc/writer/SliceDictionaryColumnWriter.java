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
import com.facebook.presto.common.type.AbstractVariableWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.facebook.presto.orc.stream.ByteArrayOutputStream;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;

public class SliceDictionaryColumnWriter
        extends DictionaryColumnWriter
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnWriter.class).instanceSize();
    private static final int DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES = toIntExact(new DataSize(32, MEGABYTE).toBytes());
    private static final int EXPECTED_ENTRIES = 1_024;

    private final AbstractVariableWidthType type;
    private ByteArrayOutputStream dictionaryDataStream;
    private LongOutputStream dictionaryLengthStream;
    private final int stringStatisticsLimitInBytes;
    private final boolean sortDictionaryKeys;

    private SliceDictionaryBuilder dictionary = new SliceDictionaryBuilder(EXPECTED_ENTRIES);
    private StringStatisticsBuilder statisticsBuilder;
    private ColumnEncoding columnEncoding;
    private SliceDirectColumnWriter directColumnWriter;

    public SliceDictionaryColumnWriter(
            int column,
            int sequence,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter)
    {
        super(column, sequence, columnWriterOptions, dwrfEncryptor, orcEncoding, metadataWriter);
        checkArgument(type instanceof AbstractVariableWidthType, "Not an instance of AbstractVariableWidthType");
        this.type = (AbstractVariableWidthType) type;
        this.dictionaryDataStream = new ByteArrayOutputStream(columnWriterOptions, dwrfEncryptor, Stream.StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        this.stringStatisticsLimitInBytes = columnWriterOptions.getStringStatisticsLimit();
        this.statisticsBuilder = newStringStatisticsBuilder();
        this.sortDictionaryKeys = columnWriterOptions.isStringDictionarySortingEnabled();
        checkState(sortDictionaryKeys || orcEncoding == DWRF, "Disabling sort is only supported in DWRF format");
    }

    @Override
    public int getDictionaryBytes()
    {
        checkState(!isDirectEncoded());
        return toIntExact(dictionary.getSizeInBytes());
    }

    @Override
    public int getDictionaryEntries()
    {
        checkState(!isDirectEncoded());
        return dictionary.getEntryCount();
    }

    @Override
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, int[] dictionaryIndexes, int maxDirectBytes)
    {
        long size = 0;
        for (int offset = 0; offset < dictionaryIndexCount; offset++) {
            int dictionaryIndex = dictionaryIndexes[offset];
            size += writeDirectEntry(dictionaryIndex);
            if (size > DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
                if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                    return false;
                }
                size = 0;
            }
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    @Override
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, short[] dictionaryIndexes, int maxDirectBytes)
    {
        long size = 0;
        for (int offset = 0; offset < dictionaryIndexCount; offset++) {
            int dictionaryIndex = dictionaryIndexes[offset];
            size += writeDirectEntry(dictionaryIndex);
            if (size > DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
                if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                    return false;
                }
                size = 0;
            }
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    @Override
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, byte[] dictionaryIndexes, int maxDirectBytes)
    {
        long size = 0;
        for (int offset = 0; offset < dictionaryIndexCount; offset++) {
            int dictionaryIndex = dictionaryIndexes[offset];
            size += writeDirectEntry(dictionaryIndex);
            if (size > DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
                if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                    return false;
                }
                size = 0;
            }
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    private long writeDirectEntry(int dictionaryIndex)
    {
        return directColumnWriter.writeBlockPosition(dictionary.getBlock(), dictionaryIndex);
    }

    @Override
    protected ColumnEncoding getDictionaryColumnEncoding()
    {
        checkState(columnEncoding != null);
        return columnEncoding;
    }

    @Override
    protected BlockStatistics addBlockToDictionary(Block block, int rowGroupOffset, int[] rowGroupIndexes)
    {
        int nonNullValueCount = 0;
        long rawBytes = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                rowGroupIndexes[rowGroupOffset++] = dictionary.putIfAbsent(block, position);
                statisticsBuilder.addValue(block, position);
                rawBytes += block.getSliceLength(position);
                nonNullValueCount++;
            }
        }
        long rawBytesIncludingNulls = rawBytes + (block.getPositionCount() - nonNullValueCount) * NULL_SIZE;
        return new BlockStatistics(nonNullValueCount, rawBytes, rawBytesIncludingNulls);
    }

    @Override
    protected void closeDictionary()
    {
        dictionary = null;
        dictionaryDataStream.close();
        dictionaryLengthStream.close();
    }

    @Override
    protected ColumnStatistics createColumnStatistics()
    {
        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        statisticsBuilder = newStringStatisticsBuilder();
        return statistics;
    }

    private static int[] getSortedDictionary(SliceDictionaryBuilder dictionary)
    {
        int[] sortedPositions = new int[dictionary.getEntryCount()];
        for (int i = 0; i < sortedPositions.length; i++) {
            sortedPositions[i] = i;
        }

        IntArrays.quickSort(sortedPositions, 0, sortedPositions.length, dictionary::compareIndex);
        return sortedPositions;
    }

    @Override
    protected void beginDataRowGroup()
    {
        directColumnWriter.beginDataRowGroup();
    }

    @Override
    protected void movePresentStreamToDirectWriter(PresentOutputStream presentStream)
    {
        directColumnWriter.updatePresentStream(presentStream);
    }

    @Override
    protected Optional<int[]> writeDictionary()
    {
        ColumnEncodingKind encodingKind = orcEncoding == DWRF ? DICTIONARY : DICTIONARY_V2;
        int dictionaryEntryCount = dictionary.getEntryCount();
        columnEncoding = new ColumnEncoding(encodingKind, dictionaryEntryCount);

        if (sortDictionaryKeys) {
            return writeSortedDictionary();
        }
        else {
            for (int i = 0; i < dictionaryEntryCount; i++) {
                writeDictionaryEntry(i);
            }
            return Optional.empty();
        }
    }

    private Optional<int[]> writeSortedDictionary()
    {
        int[] sortedDictionaryIndexes = getSortedDictionary(dictionary);
        for (int sortedDictionaryIndex : sortedDictionaryIndexes) {
            writeDictionaryEntry(sortedDictionaryIndex);
        }

        // build index from original dictionary index to new sorted position
        int[] originalDictionaryToSortedIndex = new int[sortedDictionaryIndexes.length];
        for (int sortOrdinal = 0; sortOrdinal < sortedDictionaryIndexes.length; sortOrdinal++) {
            int dictionaryIndex = sortedDictionaryIndexes[sortOrdinal];
            originalDictionaryToSortedIndex[dictionaryIndex] = sortOrdinal;
        }
        return Optional.of(originalDictionaryToSortedIndex);
    }

    private void writeDictionaryEntry(int dictionaryIndex)
    {
        int length = dictionary.getSliceLength(dictionaryIndex);
        dictionaryLengthStream.writeLong(length);
        dictionaryDataStream.writeBlockPosition(dictionary.getBlock(), dictionaryIndex, 0, length);
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            int[] rowGroupIndexes,
            Optional<int[]> optionalSortedIndex,
            LongOutputStream dataStream)
    {
        checkState(optionalSortedIndex.isPresent() == sortDictionaryKeys, "SortedIndex and sortDictionaryKeys(%s) are inconsistent", sortDictionaryKeys);

        if (sortDictionaryKeys) {
            int[] sortedIndexes = optionalSortedIndex.get();
            for (int position = 0; position < rowGroupValueCount; position++) {
                int originalDictionaryIndex = rowGroupIndexes[position];
                int sortedIndex = sortedIndexes[originalDictionaryIndex];
                writeIndex(dataStream, position, sortedIndex);
            }
        }
        else {
            for (int position = 0; position < rowGroupValueCount; position++) {
                int dictionaryIndex = rowGroupIndexes[position];
                writeIndex(dataStream, position, dictionaryIndex);
            }
        }
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            byte[] rowGroupIndexes,
            Optional<int[]> optionalSortedIndex,
            LongOutputStream dataStream)
    {
        checkState(optionalSortedIndex.isPresent() == sortDictionaryKeys, "SortedIndex and sortDictionaryKeys(%s) are inconsistent", sortDictionaryKeys);
        if (sortDictionaryKeys) {
            int[] sortedIndexes = optionalSortedIndex.get();
            for (int position = 0; position < rowGroupValueCount; position++) {
                int originalDictionaryIndex = rowGroupIndexes[position];
                int sortedIndex = sortedIndexes[originalDictionaryIndex];
                writeIndex(dataStream, position, sortedIndex);
            }
        }
        else {
            for (int position = 0; position < rowGroupValueCount; position++) {
                int dictionaryIndex = rowGroupIndexes[position];
                writeIndex(dataStream, position, dictionaryIndex);
            }
        }
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            short[] rowGroupIndexes,
            Optional<int[]> optionalSortedIndex,
            LongOutputStream dataStream)
    {
        checkState(optionalSortedIndex.isPresent() == sortDictionaryKeys, "SortedIndex and sortDictionaryKeys(%s) are inconsistent", sortDictionaryKeys);
        if (sortDictionaryKeys) {
            int[] sortedIndexes = optionalSortedIndex.get();
            for (int position = 0; position < rowGroupValueCount; position++) {
                int originalDictionaryIndex = rowGroupIndexes[position];
                int sortedIndex = sortedIndexes[originalDictionaryIndex];
                writeIndex(dataStream, position, sortedIndex);
            }
        }
        else {
            for (int position = 0; position < rowGroupValueCount; position++) {
                int dictionaryIndex = rowGroupIndexes[position];
                writeIndex(dataStream, position, dictionaryIndex);
            }
        }
    }

    private void writeIndex(LongOutputStream dataStream, int position, int dictionaryIndex)
    {
        if (dictionaryIndex < 0) {
            throw new IllegalArgumentException(String.format("Invalid index %s at position %s", dictionaryIndex, position));
        }
        dataStream.writeLong(dictionaryIndex);
    }

    @Override
    protected long getRetainedDictionaryBytes()
    {
        return INSTANCE_SIZE +
                dictionaryDataStream.getRetainedBytes() +
                dictionaryLengthStream.getRetainedBytes() +
                dictionary.getRetainedSizeInBytes() +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes());
    }

    @Override
    protected void resetDictionary()
    {
        columnEncoding = null;
        dictionary = new SliceDictionaryBuilder(EXPECTED_ENTRIES);
        dictionaryDataStream = new ByteArrayOutputStream(columnWriterOptions, dwrfEncryptor, Stream.StreamKind.DICTIONARY_DATA);
        dictionaryLengthStream = createLengthOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        statisticsBuilder = newStringStatisticsBuilder();
    }

    private StringStatisticsBuilder newStringStatisticsBuilder()
    {
        return new StringStatisticsBuilder(stringStatisticsLimitInBytes);
    }

    @Override
    protected ColumnWriter createDirectColumnWriter()
    {
        if (directColumnWriter == null) {
            directColumnWriter = new SliceDirectColumnWriter(column, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, this::newStringStatisticsBuilder, metadataWriter);
        }
        return directColumnWriter;
    }

    @Override
    protected ColumnWriter getDirectColumnWriter()
    {
        checkState(directColumnWriter != null);
        return directColumnWriter;
    }

    @Override
    protected List<StreamDataOutput> getDictionaryStreams(int column, int sequence)
    {
        return ImmutableList.of(dictionaryLengthStream.getStreamDataOutput(column, sequence), dictionaryDataStream.getStreamDataOutput(column, sequence));
    }
}
