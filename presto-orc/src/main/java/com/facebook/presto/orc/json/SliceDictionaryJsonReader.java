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
package com.facebook.presto.orc.json;

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.ByteArrayStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.OrcCorruptionException.verifyFormat;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SliceDictionaryJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean writeBinary;

    @Nonnull
    private DictionaryEntry[] dictionary = new DictionaryEntry[0];

    @Nonnull
    private int[] dictionaryLength = new int[0];

    @Nonnull
    private DictionaryEntry[] rowGroupDictionary = new DictionaryEntry[0];

    @Nonnull
    private int[] rowGroupDictionaryLength = new int[0];

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private BooleanStream inDictionaryStream;

    @Nullable
    private LongStream dataStream;

    public SliceDictionaryJsonReader(StreamDescriptor streamDescriptor, boolean writeBinary)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.writeBinary = writeBinary;
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        DictionaryEntry value = getNextValue();

        byte[] data = value.getData();
        int offset = value.getOffset();
        int length = value.length();
        if (writeBinary) {
            generator.writeBinary(data, offset, length);
        }
        else {
            generator.writeUTF8String(data, offset, length);
        }
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        DictionaryEntry value = getNextValue();

        byte[] data = value.getData();
        int offset = value.getOffset();
        int length = value.length();
        if (writeBinary) {
            return BaseEncoding.base64().encode(data, offset, length);
        }
        else {
            return new String(data, offset, length, UTF_8);
        }
    }

    private DictionaryEntry getNextValue()
            throws IOException
    {
        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

        int dictionaryIndex = Ints.checkedCast(dataStream.next());

        DictionaryEntry value;
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

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

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
                dictionary = new DictionaryEntry[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            LongStream lengthStream = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();
            verifyFormat(lengthStream != null, "Dictionary is not empty but length stream is not present");
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            ByteArrayStream dictionaryDataStream = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class).openStream();
            readDictionary(dictionaryDataStream, dictionarySize, dictionaryLength, dictionary);
        }

        presentStream = null;
        dataStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        RowGroupDictionaryLengthStream lengthStream = dataStreamSources.getStreamSource(
                streamDescriptor,
                ROW_GROUP_DICTIONARY_LENGTH,
                RowGroupDictionaryLengthStream.class).openStream();

        if (lengthStream != null) {
            inDictionaryStream = dataStreamSources.getStreamSource(streamDescriptor, IN_DICTIONARY, BooleanStream.class).openStream();

            int dictionaryEntryCount = lengthStream.getEntryCount();

            // resize the dictionary array if necessary
            if (rowGroupDictionary.length < dictionaryEntryCount) {
                rowGroupDictionary = new DictionaryEntry[dictionaryEntryCount];
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

    private static void readDictionary(ByteArrayStream dictionaryDataStream, int dictionarySize, int[] dictionaryLength, DictionaryEntry[] dictionary)
            throws IOException
    {
        // sum lengths
        int totalLength = 0;
        for (int i = 0; i < dictionarySize; i++) {
            totalLength += dictionaryLength[i];
        }

        // read dictionary data
        byte[] dictionaryData = new byte[0];
        if (totalLength > 0) {
            verifyFormat(dictionaryDataStream != null, "Dictionary length is not zero but dictionary data stream is not present");
            dictionaryData = dictionaryDataStream.next(totalLength);
        }

        // build dictionary slices
        int offset = 0;
        for (int i = 0; i < dictionarySize; i++) {
            int length = dictionaryLength[i];
            dictionary[i] = new DictionaryEntry(dictionaryData, offset, length);
            offset += length;
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private static class DictionaryEntry
    {
        private final byte[] dictionary;
        private final int offset;
        private final int length;

        public DictionaryEntry(byte[] dictionary, int offset, int length)
        {
            this.dictionary = dictionary;
            this.offset = offset;
            this.length = length;
        }

        public int length()
        {
            return length;
        }

        public byte[] getData()
        {
            return dictionary;
        }

        public int getOffset()
        {
            return offset;
        }
    }
}
