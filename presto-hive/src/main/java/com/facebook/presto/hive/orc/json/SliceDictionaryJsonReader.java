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
package com.facebook.presto.hive.orc.json;

import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.ByteArrayStream;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.OrcCorruptionException.verifyFormat;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DICTIONARY_DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class SliceDictionaryJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean writeBinary;

    @Nonnull
    private DictionaryEntry[] dictionary = new DictionaryEntry[0];

    @Nonnull
    private int[] dictionaryLength = new int[0];

    @Nullable
    private BooleanStream presentStream;

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

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

        int dictionaryIndex = Ints.checkedCast(dataStream.next());
        DictionaryEntry value = dictionary[dictionaryIndex];

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

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

        int dictionaryIndex = Ints.checkedCast(dataStream.next());
        DictionaryEntry value = dictionary[dictionaryIndex];

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
        dataStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        int dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        if (dictionarySize > 0) {

            // initialize offset and length arrays
            if (dictionary.length < dictionarySize) {
                dictionary = new DictionaryEntry[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            // read the lengths
            LongStream lengthStream = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();
            verifyFormat(lengthStream != null, "Dictionary is not empty but dictionary length stream is not present");
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            // sum lengths
            int totalLength = 0;
            for (int i = 0; i < dictionarySize; i++) {
                totalLength += dictionaryLength[i];
            }

            // read dictionary data
            byte[] dictionaryData = new byte[0];
            if (totalLength > 0) {
                ByteArrayStream dictionaryStream = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class).openStream();
                verifyFormat(dictionaryStream != null, "Dictionary length is not zero but dictionary data stream is not present");
                dictionaryData = dictionaryStream.next(totalLength);
            }

            // build dictionary slices
            int offset = 0;
            for (int i = 0; i < dictionarySize; i++) {
                int length = dictionaryLength[i];
                dictionary[i] = new DictionaryEntry(dictionaryData, offset, length);
                offset += length;
            }
        }

        presentStream = null;
        dataStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        dataStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class).openStream();
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
