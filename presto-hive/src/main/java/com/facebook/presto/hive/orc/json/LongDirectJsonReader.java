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
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.google.common.base.Preconditions.checkNotNull;

public class LongDirectJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;

    private BooleanStream presentStream;
    private LongStream dataStream;

    public LongDirectJsonReader(StreamDescriptor streamDescriptor)
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

        generator.writeNumber(dataStream.next());
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        return String.valueOf(dataStream.next());
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
        dataStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
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

        LongStreamSource dataStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, DATA, LongStreamSource.class);
        if (dataStreamSource != null) {
            dataStream = dataStreamSource.openStream();
        }
        else {
            dataStream = null;
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
