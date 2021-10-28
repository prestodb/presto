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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.TestingHiveOrcAggregatedMemoryContext;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.stream.ByteArrayInputStream;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.orc.stream.LongInputStreamV1;
import com.facebook.presto.orc.stream.LongInputStreamV2;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestSliceDictionaryColumnWriter
{
    private static final int COLUMN_ID = 1;
    private static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    private StreamDataOutput getStreamKind(List<StreamDataOutput> streams, StreamKind streamKind)
    {
        return streams.stream()
                .filter(e -> e.getStream().getStreamKind() == streamKind)
                .collect(onlyElement());
    }

    private Optional<OrcDecompressor> getOrcDecompressor()
    {
        int compressionBlockSize = toIntExact(new DataSize(256, KILOBYTE).toBytes());
        return createOrcDecompressor(ORC_DATA_SOURCE_ID, SNAPPY, compressionBlockSize);
    }

    private OrcInputStream convertSliceToInputStream(Slice slice)
    {
        TestingHiveOrcAggregatedMemoryContext aggregatedMemoryContext = new TestingHiveOrcAggregatedMemoryContext();
        return new OrcInputStream(
                ORC_DATA_SOURCE_ID,
                new SharedBuffer(aggregatedMemoryContext.newOrcLocalMemoryContext("sharedDecompressionBuffer")),
                slice.getInput(),
                getOrcDecompressor(),
                Optional.empty(),
                aggregatedMemoryContext,
                slice.getRetainedSize());
    }

    private Slice convertStreamToSlice(StreamDataOutput streamDataOutput)
            throws OrcCorruptionException
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(toIntExact(streamDataOutput.size()));
        streamDataOutput.writeData(sliceOutput);
        return sliceOutput.slice();
    }

    private OrcInputStream getOrcInputStream(List<StreamDataOutput> streams, StreamKind streamKind)
            throws OrcCorruptionException
    {
        StreamDataOutput stream = getStreamKind(streams, streamKind);
        Slice slice = convertStreamToSlice(stream);
        return convertSliceToInputStream(slice);
    }

    private LongInputStream getDictionaryLengthStream(List<StreamDataOutput> streams, OrcEncoding orcEncoding)
    {
        if (orcEncoding == DWRF) {
            return new LongInputStreamV1(getOrcInputStream(streams, LENGTH), false);
        }
        return new LongInputStreamV2(getOrcInputStream(streams, LENGTH), false, false);
    }

    private List<String> getDictionaryKeys(List<String> values, OrcEncoding orcEncoding, boolean sortDictionaryKeys)
            throws IOException
    {
        DictionaryColumnWriter writer = getDictionaryColumnWriter(orcEncoding, sortDictionaryKeys);

        for (int index = 0; index < values.size(); ) {
            int endIndex = Math.min(index + 10_000, values.size());

            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10_000);
            while (index < endIndex) {
                VARCHAR.writeSlice(blockBuilder, utf8Slice(values.get(index++)));
            }

            writer.beginRowGroup();
            writer.writeBlock(blockBuilder);
            writer.finishRowGroup();
        }

        writer.close();
        List<StreamDataOutput> streams = writer.getDataStreams();
        int dictionarySize = writer.getColumnEncodings().get(COLUMN_ID).getDictionarySize();
        ByteArrayInputStream dictionaryDataStream = new ByteArrayInputStream(getOrcInputStream(streams, DICTIONARY_DATA));
        LongInputStream dictionaryLengthStream = getDictionaryLengthStream(streams, orcEncoding);
        List<String> dictionaryKeys = new ArrayList<>(dictionarySize);
        for (int i = 0; i < dictionarySize; i++) {
            int length = toIntExact(dictionaryLengthStream.next());
            String dictionaryKey = new String(dictionaryDataStream.next(length), UTF_8);
            dictionaryKeys.add(dictionaryKey);
        }
        return dictionaryKeys;
    }

    private DictionaryColumnWriter getDictionaryColumnWriter(OrcEncoding orcEncoding, boolean sortDictionaryKeys)
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(SNAPPY)
                .setStringDictionarySortingEnabled(sortDictionaryKeys)
                .build();
        DictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                COLUMN_ID,
                VARCHAR,
                columnWriterOptions,
                Optional.empty(),
                orcEncoding,
                orcEncoding.createMetadataWriter());
        return writer;
    }

    @Test
    public void testSortedDictionaryKeys()
            throws IOException
    {
        for (OrcEncoding orcEncoding : OrcEncoding.values()) {
            List<String> sortedKeys = getDictionaryKeys(ImmutableList.of("b", "a", "c"), orcEncoding, true);
            assertEquals(sortedKeys, ImmutableList.of("a", "b", "c"));

            sortedKeys = getDictionaryKeys(ImmutableList.of("b", "b", "a"), orcEncoding, true);
            assertEquals(sortedKeys, ImmutableList.of("a", "b"));
        }
    }

    @Test
    public void testUnsortedDictionaryKeys()
            throws IOException
    {
        List<String> sortedKeys = getDictionaryKeys(ImmutableList.of("b", "a", "c"), DWRF, false);
        assertEquals(sortedKeys, ImmutableList.of("b", "a", "c"));

        sortedKeys = getDictionaryKeys(ImmutableList.of("b", "b", "a"), DWRF, false);
        assertEquals(sortedKeys, ImmutableList.of("b", "a"));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testOrcStringSortingDisabledThrows()
    {
        getDictionaryColumnWriter(ORC, false);
    }

    @Test
    public void testStringDirectConversion()
    {
        // a single row group exceeds 2G after direct conversion
        byte[] value = new byte[megabytes(1)];
        ThreadLocalRandom.current().nextBytes(value);
        Block data = RunLengthEncodedBlock.create(VARCHAR, Slices.wrappedBuffer(value), 3000);

        for (OrcEncoding orcEncoding : OrcEncoding.values()) {
            DictionaryColumnWriter writer = getDictionaryColumnWriter(orcEncoding, true);

            writer.beginRowGroup();
            writer.writeBlock(data);
            writer.finishRowGroup();
            assertFalse(writer.tryConvertToDirect(megabytes(64)).isPresent());
        }
    }

    private static int megabytes(int size)
    {
        return toIntExact(new DataSize(size, MEGABYTE).toBytes());
    }
}
