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
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.array.Arrays;
import com.facebook.presto.orc.array.IntBigArray;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.LongOutputStreamDwrf;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class LongDictionaryColumnWriter
        extends DictionaryColumnWriter
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(LongDictionaryColumnWriter.class).instanceSize();
    private static final int NULL_INDEX = -1;

    private final LongOutputStream dictionaryDataStream;
    private final int typeSize;

    private LongDictionary dictionary;
    private IntegerStatisticsBuilder statisticsBuilder;
    private ColumnEncoding columnEncoding;
    private LongColumnWriter directColumnWriter;
    private long[] directValues;
    private boolean[] directNulls;

    public LongDictionaryColumnWriter(
            int column,
            Type type,
            CompressionParameters compressionParameters,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter)
    {
        super(column, type, compressionParameters, dwrfEncryptor, orcEncoding, metadataWriter);
        checkArgument(orcEncoding == DWRF, "Long dictionary encoding is only supported in DWRF");
        checkArgument(type instanceof FixedWidthType, "Not a fixed width type");
        this.dictionaryDataStream = new LongOutputStreamDwrf(compressionParameters, dwrfEncryptor, true, DICTIONARY_DATA);
        this.dictionary = new LongDictionary(10_000);
        this.typeSize = ((FixedWidthType) type).getFixedSize();
        this.statisticsBuilder = new IntegerStatisticsBuilder();
    }

    @Override
    public int getDictionaryEntries()
    {
        return dictionary.size();
    }

    @Override
    public int getDictionaryBytes()
    {
        return dictionary.size() * typeSize;
    }

    @Override
    protected ColumnWriter createDirectColumnWriter()
    {
        if (directColumnWriter == null) {
            directColumnWriter = new LongColumnWriter(column, type, compressionParameters, dwrfEncryptor, orcEncoding, IntegerStatisticsBuilder::new, metadataWriter);
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
    protected boolean tryConvertToDirect(int dictionaryIndexCount, IntBigArray dictionaryIndexes, int maxDirectBytes)
    {
        int[][] segments = dictionaryIndexes.getSegments();
        for (int i = 0; dictionaryIndexCount > 0 && i < segments.length; i++) {
            int[] segment = segments[i];
            int positionCount = Math.min(dictionaryIndexCount, segment.length);
            directValues = Arrays.ensureCapacity(directValues, positionCount);
            directNulls = Arrays.ensureCapacity(directNulls, positionCount);
            for (int j = 0; j < positionCount; j++) {
                if (segment[j] != NULL_INDEX) {
                    directValues[j] = dictionary.getValue(segment[j]);
                    directNulls[j] = false;
                }
                else {
                    directNulls[j] = true;
                }
            }

            LongArrayBlock longArrayBlock = new LongArrayBlock(positionCount, Optional.of(directNulls), directValues);
            directColumnWriter.writeBlock(longArrayBlock);
            if (directColumnWriter.getBufferedBytes() > maxDirectBytes) {
                return false;
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
            if (block.isNull(position)) {
                rowGroupIndexes.set(rowGroupValueCount, NULL_INDEX);
            }
            else {
                long value = type.getLong(block, position);
                int index = dictionary.addIfNotExists(value);
                rowGroupIndexes.set(rowGroupValueCount, index);
                statisticsBuilder.addValue(value);
                rawBytes += typeSize;
                nonNullValueCount++;
            }
            rowGroupValueCount++;
        }
        return new BlockStatistics(nonNullValueCount, rawBytes);
    }

    @Override
    protected long getRetainedDictionaryBytes()
    {
        return INSTANCE_SIZE +
                dictionary.getRetainedBytes() +
                dictionaryDataStream.getRetainedBytes() +
                sizeOf(directValues) +
                sizeOf(directNulls) +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes());
    }

    @Override
    protected Optional<int[]> writeDictionary()
    {
        long[] elements = dictionary.elements();
        for (int i = 0; i < dictionary.size(); i++) {
            dictionaryDataStream.writeLong(elements[i]);
        }

        columnEncoding = new ColumnEncoding(DICTIONARY, dictionary.size());
        return Optional.empty();
    }

    @Override
    protected void writePresentAndDataStreams(
            int rowGroupValueCount,
            IntBigArray rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            PresentOutputStream presentStream,
            LongOutputStream dataStream)
    {
        checkArgument(!originalDictionaryToSortedIndex.isPresent(), "Unsupported originalDictionaryToSortedIndex");
        for (int position = 0; position < rowGroupValueCount; position++) {
            presentStream.writeBoolean(rowGroupIndexes.get(position) != NULL_INDEX);
        }
        for (int position = 0; position < rowGroupValueCount; position++) {
            int index = rowGroupIndexes.get(position);
            if (index != NULL_INDEX) {
                dataStream.writeLong(index);
            }
        }
    }

    @Override
    protected List<StreamDataOutput> getDictionaryStreams(int column)
    {
        return ImmutableList.of(dictionaryDataStream.getStreamDataOutput(column));
    }

    @Override
    protected ColumnStatistics createColumnStatistics()
    {
        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        statisticsBuilder = new IntegerStatisticsBuilder();
        return statistics;
    }

    @Override
    protected void closeDictionary()
    {
        dictionary = null;
        dictionaryDataStream.close();
    }

    @Override
    protected void resetDictionary()
    {
        columnEncoding = null;
        dictionary = new LongDictionary(10_000);
        dictionaryDataStream.reset();
        statisticsBuilder = new IntegerStatisticsBuilder();
        directValues = null;
        directNulls = null;
    }

    private static class Long2IntMapWithByteSize
            extends Long2IntOpenHashMap
    {
        public Long2IntMapWithByteSize(int expectedEntries, int unusedValue)
        {
            super(expectedEntries);
            this.defaultReturnValue(unusedValue);
        }

        public long getRetainedBytes()
        {
            return sizeOf(key) + sizeOf(value);
        }
    }

    private static class LongDictionary
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionary.class).instanceSize() +
                ClassLayout.parseClass(Long2IntMapWithByteSize.class).instanceSize() +
                ClassLayout.parseClass(LongArrayList.class).instanceSize();

        private static final int UNUSED_VALUE = -1;
        private final Long2IntMapWithByteSize dictionary;
        private final LongArrayList elements;

        public LongDictionary(int expectedEntries)
        {
            this.dictionary = new Long2IntMapWithByteSize(expectedEntries, UNUSED_VALUE);
            this.elements = new LongArrayList(expectedEntries);
        }

        public long getRetainedBytes()
        {
            return INSTANCE_SIZE + dictionary.getRetainedBytes() + sizeOf(elements.elements());
        }

        public int addIfNotExists(long value)
        {
            int newPosition = dictionary.size();
            int existingPosition = dictionary.putIfAbsent(value, newPosition);
            if (existingPosition == UNUSED_VALUE) {
                elements.add(value);
                return newPosition;
            }
            return existingPosition;
        }

        public int size()
        {
            checkState(elements.size() == dictionary.size(), "size mismatch");
            return elements.size();
        }

        public long[] elements()
        {
            return elements.elements();
        }

        public long getValue(int index)
        {
            return elements.getLong(index);
        }
    }
}
