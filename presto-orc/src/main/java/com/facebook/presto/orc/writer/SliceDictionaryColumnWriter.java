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
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.array.IntBigArray;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressionParameters;
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
import static java.util.Objects.requireNonNull;

public class SliceDictionaryColumnWriter
        extends DictionaryColumnWriter
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnWriter.class).instanceSize();
    private static final int DIRECT_CONVERSION_CHUNK_MAX_LOGICAL_BYTES = toIntExact(new DataSize(32, MEGABYTE).toBytes());

    private final ByteArrayOutputStream dictionaryDataStream;
    private final LongOutputStream dictionaryLengthStream;
    private final DictionaryBuilder dictionary = new DictionaryBuilder(10000);
    private final int stringStatisticsLimitInBytes;

    private StringStatisticsBuilder statisticsBuilder;
    private ColumnEncoding columnEncoding;
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
        super(column, type, compressionParameters, dwrfEncryptor, orcEncoding, metadataWriter);
        this.dictionaryDataStream = new ByteArrayOutputStream(compressionParameters, dwrfEncryptor, Stream.StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(compressionParameters, dwrfEncryptor, orcEncoding);
        this.stringStatisticsLimitInBytes = toIntExact(requireNonNull(stringStatisticsLimit, "stringStatisticsLimit is null").toBytes());
        this.statisticsBuilder = newStringStatisticsBuilder();
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
    protected boolean tryConvertToDirect(int dictionaryIndexCount, IntBigArray dictionaryIndexes, int maxDirectBytes)
    {
        int[][] segments = dictionaryIndexes.getSegments();
        for (int i = 0; dictionaryIndexCount > 0 && i < segments.length; i++) {
            int[] segment = segments[i];
            int positionCount = Math.min(dictionaryIndexCount, segment.length);
            Block block = new DictionaryBlock(positionCount, dictionary.getElementBlock(), segment);

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

            dictionaryIndexCount -= positionCount;
        }
        checkState(dictionaryIndexCount == 0);
        return true;
    }

    @Override
    protected ColumnEncoding getDictionaryColumnEncoding()
    {
        checkState(columnEncoding != null);
        return columnEncoding;
    }

    @Override
    protected BlockStatistics addBlockToDictionary(Block block, int rowGroupValueCount, IntBigArray rowGroupIndexes)
    {
        int nonNullValueCount = 0;
        long rawBytes = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            int index = dictionary.putIfAbsent(block, position);
            rowGroupIndexes.set(rowGroupValueCount, index);
            rowGroupValueCount++;

            if (!block.isNull(position)) {
                // todo min/max statistics only need to be updated if value was not already in the dictionary, but non-null count does
                statisticsBuilder.addValue(type.getSlice(block, position));

                rawBytes += block.getSliceLength(position);
                nonNullValueCount++;
            }
        }
        return new BlockStatistics(nonNullValueCount, rawBytes);
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

    private static int[] getSortedDictionaryNullsLast(Block elementBlock)
    {
        int[] sortedPositions = new int[elementBlock.getPositionCount()];
        for (int i = 0; i < sortedPositions.length; i++) {
            sortedPositions[i] = i;
        }

        IntArrays.quickSort(sortedPositions, 0, sortedPositions.length, (left, right) -> {
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
        });

        return sortedPositions;
    }

    @Override
    protected Optional<int[]> writeDictionary()
    {
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
        return Optional.of(originalDictionaryToSortedIndex);
    }

    @Override
    protected void writePresentAndDataStreams(
            int rowGroupValueCount,
            IntBigArray rowGroupIndexes,
            Optional<int[]> optionalSortedIndex,
            PresentOutputStream presentStream,
            LongOutputStream dataStream)
    {
        checkState(optionalSortedIndex.isPresent(), "originalDictionaryToSortedIndex is null");
        int[] originalDictionaryToSortedIndex = optionalSortedIndex.get();
        for (int position = 0; position < rowGroupValueCount; position++) {
            presentStream.writeBoolean(rowGroupIndexes.get(position) != 0);
        }
        for (int position = 0; position < rowGroupValueCount; position++) {
            int originalDictionaryIndex = rowGroupIndexes.get(position);
            // index zero in original dictionary is reserved for null
            if (originalDictionaryIndex != 0) {
                int sortedIndex = originalDictionaryToSortedIndex[originalDictionaryIndex];
                if (sortedIndex < 0) {
                    throw new IllegalArgumentException();
                }
                dataStream.writeLong(sortedIndex);
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
            directColumnWriter = new SliceDirectColumnWriter(column, type, compressionParameters, dwrfEncryptor, orcEncoding, this::newStringStatisticsBuilder, metadataWriter);
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
