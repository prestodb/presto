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
package com.facebook.presto.orc.block;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.ByteArrayStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class SliceDictionaryBlockReader
        implements BlockReader
{
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final StreamDescriptor streamDescriptor;

    @Nonnull
    private byte[][] dictionary = new byte[0][];

    @Nonnull
    private int[] dictionaryLength = new int[0];

    @Nonnull
    private byte[][] rowGroupDictionary = new byte[0][];

    @Nonnull
    private int[] rowGroupDictionaryLength = new int[0];

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private BooleanStream inDictionaryStream;

    @Nullable
    private LongStream dataStream;

    public SliceDictionaryBlockReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public boolean readNextValueInto(BlockBuilder builder, boolean skipNull)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            if (!skipNull) {
                builder.appendNull();
                return true;
            }
            return false;
        }

        VARCHAR.writeSlice(builder, Slices.wrappedBuffer(getNextValue()));
        return true;
    }

    private byte[] getNextValue()
            throws IOException
    {
        if (dataStream == null) {
            throw new OrcCorruptionException("Value is not null but data stream is not present");
        }

        int dictionaryIndex = Ints.checkedCast(dataStream.next());

        byte[] value;
        if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
            value = dictionary[dictionaryIndex];
        }
        else {
            value = rowGroupDictionary[dictionaryIndex];
        }
        return value;
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0) {
            return;
        }

        if (dataStream == null) {
            throw new OrcCorruptionException("Value is not null but data stream is not present");
        }

        // skip non-null length
        if (inDictionaryStream != null) {
            inDictionaryStream.skip(skipSize);
        }
        dataStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        int dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        if (dictionarySize > 0) {
            // resize the dictionary array if necessary
            if (dictionary.length < dictionarySize) {
                dictionary = new byte[dictionarySize][];
                dictionaryLength = new int[dictionarySize];
            }

            LongStream lengthStream = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();
            if (lengthStream == null) {
                throw new OrcCorruptionException("Dictionary is not empty but length stream is not present");
            }
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            ByteArrayStream dictionaryDataStream = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class).openStream();
            readDictionary(dictionaryDataStream, dictionarySize, dictionaryLength, dictionary);
        }

        presentStream = null;
        dataStream = null;
        inDictionaryStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        RowGroupDictionaryLengthStream lengthStream = dataStreamSources.getStreamSource(
                streamDescriptor,
                ROW_GROUP_DICTIONARY_LENGTH,
                RowGroupDictionaryLengthStream.class).openStream();

        if (lengthStream == null) {
            inDictionaryStream = null;
        }
        else {
            inDictionaryStream = dataStreamSources.getStreamSource(streamDescriptor, IN_DICTIONARY, BooleanStream.class).openStream();

            int dictionaryEntryCount = lengthStream.getEntryCount();

            // resize the dictionary array if necessary
            if (rowGroupDictionary.length < dictionaryEntryCount) {
                rowGroupDictionary = new byte[dictionaryEntryCount][];
                rowGroupDictionaryLength = new int[dictionaryEntryCount];
            }

            // read the lengths
            lengthStream.nextIntVector(dictionaryEntryCount, rowGroupDictionaryLength);

            ByteArrayStream dictionaryDataStream = dataStreamSources.getStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY, ByteArrayStream.class).openStream();
            readDictionary(dictionaryDataStream, dictionaryEntryCount, rowGroupDictionaryLength, rowGroupDictionary);
        }

        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        dataStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class).openStream();
    }

    private static void readDictionary(ByteArrayStream dictionaryDataStream, int dictionarySize, int[] dictionaryLength, byte[][] dictionary)
            throws IOException
    {
        // build dictionary slices
        for (int i = 0; i < dictionarySize; i++) {
            int length = dictionaryLength[i];
            if (length == 0) {
                dictionary[i] = EMPTY_BYTE_ARRAY;
            }
            else {
                dictionary[i] = dictionaryDataStream.next(length);
            }
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
