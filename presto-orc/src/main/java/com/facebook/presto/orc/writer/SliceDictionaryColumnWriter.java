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
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;

public class SliceDictionaryColumnWriter
        extends DictionaryColumnWriter
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnWriter.class).instanceSize();
    private static final int DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES = toIntExact(new DataSize(32, MEGABYTE).toBytes());
    private static final int NULL_INDEX = -1;

    private final ByteArrayOutputStream dictionaryDataStream;
    private final LongOutputStream dictionaryLengthStream;
    private final SliceDictionaryBuilder dictionary = new SliceDictionaryBuilder(10_000);
    private final int stringStatisticsLimitInBytes;
    private final boolean sortDictionaryKeys;

    private StringStatisticsBuilder statisticsBuilder;
    private ColumnEncoding columnEncoding;
    private SliceDirectColumnWriter directColumnWriter;

    public SliceDictionaryColumnWriter(
            int column,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter)
    {
        super(column, type, columnWriterOptions, dwrfEncryptor, orcEncoding, metadataWriter);
        this.dictionaryDataStream = new ByteArrayOutputStream(columnWriterOptions, dwrfEncryptor, Stream.StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(columnWriterOptions, dwrfEncryptor, orcEncoding);
        this.stringStatisticsLimitInBytes = toIntExact(columnWriterOptions.getStringStatisticsLimit().toBytes());
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
        for (int offset = 0; offset < dictionaryIndexCount; offset++) {
            directColumnWriter.writePresentValue(dictionaryIndexes[offset] != NULL_INDEX);
        }
        long size = 0;
        for (int offset = 0; offset < dictionaryIndexCount; offset++) {
            int dictionaryIndex = dictionaryIndexes[offset];
            if (dictionaryIndex != NULL_INDEX) {
                int length = dictionary.getSliceLength(dictionaryIndex);
                Slice rawSlice = dictionary.getRawSlice(dictionaryIndex);
                int rawSliceOffset = dictionary.getRawSliceOffset(dictionaryIndex);
                directColumnWriter.writeSlice(rawSlice, rawSliceOffset, length);
                size += length;
                if (size > DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES) {
                    if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                        return false;
                    }
                    size = 0;
                }
            }
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    @Override
    protected ColumnEncoding getDictionaryColumnEncoding()
    {
        checkState(columnEncoding != null);
        return columnEncoding;
    }

    @Override
    protected BlockStatistics addBlockToDictionary(Block block, int rowGroupValueCount, int[] rowGroupIndexes)
    {
        int nonNullValueCount = 0;
        long rawBytes = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            int index;
            if (!block.isNull(position)) {
                index = dictionary.putIfAbsent(block, position);

                // todo min/max statistics only need to be updated if value was not already in the dictionary, but non-null count does
                Slice slice = type.getSlice(block, position);
                statisticsBuilder.addValue(slice, 0, slice.length());

                rawBytes += block.getSliceLength(position);
                nonNullValueCount++;
            }
            else {
                index = NULL_INDEX;
            }
            rowGroupIndexes[rowGroupValueCount++] = index;
        }
        long rawBytesIncludingNulls = rawBytes + (block.getPositionCount() - nonNullValueCount) * NULL_SIZE;
        return new BlockStatistics(nonNullValueCount, rawBytes, rawBytesIncludingNulls);
    }

    @Override
    protected void closeDictionary()
    {
        // free the dictionary memory
        dictionary.clear();

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
        Slice rawSlice = dictionary.getRawSlice(dictionaryIndex);
        int rawSliceOffset = dictionary.getRawSliceOffset(dictionaryIndex);
        dictionaryDataStream.writeSlice(rawSlice, rawSliceOffset, length);
    }

    @Override
    protected void writePresentAndDataStreams(
            int rowGroupValueCount,
            int[] rowGroupIndexes,
            Optional<int[]> optionalSortedIndex,
            PresentOutputStream presentStream,
            LongOutputStream dataStream)
    {
        checkState(optionalSortedIndex.isPresent() == sortDictionaryKeys, "SortedIndex and sortDictionaryKeys(%s) are inconsistent", sortDictionaryKeys);
        for (int position = 0; position < rowGroupValueCount; position++) {
            presentStream.writeBoolean(rowGroupIndexes[position] != NULL_INDEX);
        }

        if (sortDictionaryKeys) {
            int[] sortedIndexes = optionalSortedIndex.get();
            for (int position = 0; position < rowGroupValueCount; position++) {
                int originalDictionaryIndex = rowGroupIndexes[position];
                if (originalDictionaryIndex != NULL_INDEX) {
                    int sortedIndex = sortedIndexes[originalDictionaryIndex];
                    if (sortedIndex < 0) {
                        throw new IllegalArgumentException(String.format("Invalid index %s at position %s", sortedIndex, position));
                    }
                    dataStream.writeLong(sortedIndex);
                }
            }
        }
        else {
            for (int position = 0; position < rowGroupValueCount; position++) {
                int dictionaryIndex = rowGroupIndexes[position];
                if (dictionaryIndex != NULL_INDEX) {
                    if (dictionaryIndex < 0) {
                        throw new IllegalArgumentException(String.format("Invalid index %s at position %s", dictionaryIndex, position));
                    }
                    dataStream.writeLong(dictionaryIndex);
                }
            }
        }
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
        dictionary.clear();
        dictionaryDataStream.reset();
        dictionaryLengthStream.reset();
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
            directColumnWriter = new SliceDirectColumnWriter(column, type, columnWriterOptions, dwrfEncryptor, orcEncoding, this::newStringStatisticsBuilder, metadataWriter);
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
    protected List<StreamDataOutput> getDictionaryStreams(int column)
    {
        return ImmutableList.of(dictionaryLengthStream.getStreamDataOutput(column), dictionaryDataStream.getStreamDataOutput(column));
    }
}
