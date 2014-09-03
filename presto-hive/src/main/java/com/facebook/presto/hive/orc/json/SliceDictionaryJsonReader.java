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
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.ByteArrayStream;
import com.facebook.presto.hive.orc.stream.ByteArrayStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.misc.Unsafe;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.LENGTH;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SliceDictionaryJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean writeBinary;

    private Slice[] dictionary = new Slice[0];
    private int[] dictionaryLength = new int[0];

    private BooleanStream presentStream;
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

        int dictionaryIndex = Ints.checkedCast(dataStream.next());
        Slice value = dictionary[dictionaryIndex];
        int length = value.length();
        if (length == 0) {
            generator.writeString("");
        }
        else {
            byte[] data = (byte[]) value.getBase();
            int offset = (int) (value.getAddress() - Unsafe.ARRAY_BYTE_BASE_OFFSET);
            if (writeBinary) {
                generator.writeBinary(data, offset, length);
            }
            else {
                generator.writeUTF8String(data, offset, length);
            }
        }
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        int dictionaryIndex = Ints.checkedCast(dataStream.next());
        Slice value = dictionary[dictionaryIndex];
        int length = value.length();
        if (length == 0) {
            return "";
        }

        byte[] data = (byte[]) value.getBase();
        int offset = (int) (value.getAddress() - Unsafe.ARRAY_BYTE_BASE_OFFSET);
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

        // skip non-null length
        dataStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ByteArrayStreamSource dictionaryDataStreamSource = dictionaryStreamSources.getStreamSourceIfPresent(streamDescriptor, DICTIONARY_DATA, ByteArrayStreamSource.class);
        if (dictionaryDataStreamSource != null) {
            int dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();

            // initialize offset and length arrays
            if (dictionary.length < dictionarySize) {
                dictionary = new Slice[dictionarySize];
                dictionaryLength = new int[dictionarySize];
            }

            // read the lengths
            LongStream lengthStream = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStreamSource.class).openStream();
            lengthStream.nextIntVector(dictionarySize, dictionaryLength);

            // sum lengths
            int totalLength = 0;
            for (int i = 0; i < dictionarySize; i++) {
                totalLength += dictionaryLength[i];
            }

            // read dictionary data
            ByteArrayStream dictionaryStream = dictionaryDataStreamSource.openStream();
            byte[] dictionaryData = dictionaryStream.next(totalLength);

            // build dictionary slices
            int offset = 0;
            for (int i = 0; i < dictionarySize; i++) {
                int length = dictionaryLength[i];
                dictionary[i] = Slices.wrappedBuffer(dictionaryData, offset, length);
                offset += length;
            }
        }
        else {
            dictionary = new Slice[0];
        }

        presentStream = null;
        dataStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        BooleanStreamSource presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        if (presentStreamSource != null) {
            presentStream = presentStreamSource.openStream();
        }
        else {
            presentStream = null;
        }

        dataStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStreamSource.class).openStream();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
