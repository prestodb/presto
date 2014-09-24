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
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.IN_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.google.common.base.Preconditions.checkNotNull;

public class LongDictionaryJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;

    private BooleanStream presentStream;
    private BooleanStream inDictionaryStream;
    private LongStream dataStream;

    private long[] dictionary = new long[0];

    public LongDictionaryJsonReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        generator.writeNumber(nextValue());
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        return String.valueOf(nextValue());
    }

    private long nextValue()
            throws IOException
    {
        long value = dataStream.next();
        if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
            value = dictionary[((int) value)];
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

        // skip non-null values
        if (inDictionaryStream != null) {
            inDictionaryStream.skip(skipSize);
        }
        dataStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        LongStreamSource dictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, LongStreamSource.class);
        if (dictionaryDataStreamSource != null) {
            int dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();

            if (dictionary.length < dictionarySize) {
                dictionary = new long[dictionarySize];
            }

            // read dictionary data
            LongStream dictionaryStream = dictionaryDataStreamSource.openStream();
            dictionaryStream.nextLongVector(dictionarySize, dictionary);
        }
        else {
            dictionary = new long[0];
        }

        presentStream = null;
        inDictionaryStream = null;
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

        BooleanStreamSource inDictionaryStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, IN_DICTIONARY, BooleanStreamSource.class);
        if (inDictionaryStreamSource != null) {
            inDictionaryStream = inDictionaryStreamSource.openStream();
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
