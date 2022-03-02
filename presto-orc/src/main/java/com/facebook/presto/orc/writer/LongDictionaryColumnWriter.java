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
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.LongOutputStreamDwrf;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class LongDictionaryColumnWriter
        extends DictionaryColumnWriter
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(LongDictionaryColumnWriter.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10_000;

    private final FixedWidthType type;
    private final long typeSize;

    private LongOutputStream dictionaryDataStream;
    private LongDictionaryBuilder dictionary;
    private IntegerStatisticsBuilder statisticsBuilder;
    private ColumnEncoding columnEncoding;
    private LongColumnWriter directColumnWriter;

    public LongDictionaryColumnWriter(
            int column,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            MetadataWriter metadataWriter)
    {
        super(column, type, columnWriterOptions, dwrfEncryptor, orcEncoding, metadataWriter);
        checkArgument(orcEncoding == DWRF, "Long dictionary encoding is only supported in DWRF");
        checkArgument(type instanceof FixedWidthType, "Not a fixed width type");
        this.type = (FixedWidthType) type;
        this.typeSize = this.type.getFixedSize();

        this.dictionaryDataStream = new LongOutputStreamDwrf(columnWriterOptions, dwrfEncryptor, true, DICTIONARY_DATA);
        this.dictionary = new LongDictionaryBuilder(EXPECTED_ENTRIES);
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
        // This method measures the dictionary size required for the reader to decode.
        // The reader uses long[] array to hold the contents of the dictionary.
        // @See com.facebook.presto.orc.reader.LongDictionarySelectiveStreamReader.dictionary
        // So always multiply by the Long.BYTES size instead of typeSize.
        return dictionary.size() * Long.BYTES;
    }

    @Override
    protected ColumnWriter createDirectColumnWriter()
    {
        if (directColumnWriter == null) {
            directColumnWriter = new LongColumnWriter(column, type, columnWriterOptions, dwrfEncryptor, orcEncoding, IntegerStatisticsBuilder::new, metadataWriter);
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
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, int[] dictionaryIndexes, int maxDirectBytes)
    {
        for (int i = 0; i < dictionaryIndexCount; i++) {
            writeIndex(dictionaryIndexes[i]);
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    @Override
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, short[] dictionaryIndexes, int maxDirectBytes)
    {
        for (int i = 0; i < dictionaryIndexCount; i++) {
            writeIndex(dictionaryIndexes[i]);
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    @Override
    protected boolean tryConvertRowGroupToDirect(int dictionaryIndexCount, byte[] dictionaryIndexes, int maxDirectBytes)
    {
        for (int i = 0; i < dictionaryIndexCount; i++) {
            writeIndex(dictionaryIndexes[i]);
        }

        return directColumnWriter.getBufferedBytes() <= maxDirectBytes;
    }

    void writeIndex(int index)
    {
        directColumnWriter.writeValue(dictionary.getValue(index));
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
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                long value = type.getLong(block, position);
                statisticsBuilder.addValue(value);
                rowGroupIndexes[rowGroupOffset++] = dictionary.putIfAbsent(value);
                nonNullValueCount++;
            }
        }
        long rawBytesIncludingNulls = (nonNullValueCount * typeSize) +
                (block.getPositionCount() - nonNullValueCount) * NULL_SIZE;

        long rawBytesEstimate = 0;
        if (nonNullValueCount > 0) {
            // For Long Dictionary encoding is useful when the values are large integers.
            // Say if all values are less than 256 there is no space savings. Instead, it makes
            // the reader worse by encoding data and dictionary streams separately. The raw bytes
            // estimate is a way to understand how many bytes would be required, if it was encoded
            // using direct encoding. The estimate currently takes max for a row group and assumes
            // it is a representative number to calculate the bytes required to encode each value.
            // This is a heuristic, so it is possible to craft test cases to make wrong assumptions.
            // This heuristic worked well for all the cases where there were problems.
            // Couple of other alternatives considered are min and average.
            // In Warehouse most of the columns examined has min of 0, so min alone is not a good
            // value by itself. Sum can overflow and sum could be missing. Doing for each value
            // is CPU intensive, but max seems to be good measure to start with.
            int perValueBits = 64 - Long.numberOfLeadingZeros(statisticsBuilder.getMaximum());
            long perValueBytes = perValueBits / 8 + 1;
            rawBytesEstimate = perValueBytes * nonNullValueCount;
        }
        return new BlockStatistics(nonNullValueCount, rawBytesEstimate, rawBytesIncludingNulls);
    }

    @Override
    protected long getRetainedDictionaryBytes()
    {
        return INSTANCE_SIZE +
                dictionary.getRetainedBytes() +
                dictionaryDataStream.getRetainedBytes() +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes());
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
        long[] elements = dictionary.elements();
        for (int i = 0; i < dictionary.size(); i++) {
            dictionaryDataStream.writeLong(elements[i]);
        }

        columnEncoding = new ColumnEncoding(DICTIONARY, dictionary.size());
        return Optional.empty();
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            int[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream)
    {
        checkArgument(!originalDictionaryToSortedIndex.isPresent(), "Unsupported originalDictionaryToSortedIndex");
        for (int position = 0; position < rowGroupValueCount; position++) {
            int index = rowGroupIndexes[position];
            dataStream.writeLong(index);
        }
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            short[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream)
    {
        checkArgument(!originalDictionaryToSortedIndex.isPresent(), "Unsupported originalDictionaryToSortedIndex");
        for (int position = 0; position < rowGroupValueCount; position++) {
            int index = rowGroupIndexes[position];
            dataStream.writeLong(index);
        }
    }

    @Override
    protected void writeDataStreams(
            int rowGroupValueCount,
            byte[] rowGroupIndexes,
            Optional<int[]> originalDictionaryToSortedIndex,
            LongOutputStream dataStream)
    {
        checkArgument(!originalDictionaryToSortedIndex.isPresent(), "Unsupported originalDictionaryToSortedIndex");
        for (int position = 0; position < rowGroupValueCount; position++) {
            int index = rowGroupIndexes[position];
            dataStream.writeLong(index);
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
        dictionary = new LongDictionaryBuilder(EXPECTED_ENTRIES);
        dictionaryDataStream = new LongOutputStreamDwrf(columnWriterOptions, dwrfEncryptor, true, DICTIONARY_DATA);
        statisticsBuilder = new IntegerStatisticsBuilder();
    }
}
