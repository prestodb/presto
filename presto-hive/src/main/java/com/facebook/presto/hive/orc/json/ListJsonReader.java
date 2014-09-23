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

import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.json.JsonReaders.createJsonReader;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class ListJsonReader
        implements JsonReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;

    private BooleanStream presentStream;
    private LongStream lengthStream;
    private final JsonReader elementReader;

    public ListJsonReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        elementReader = createJsonReader(streamDescriptor.getNestedStreams().get(0), true, sessionTimeZone);
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        long length = lengthStream.next();
        generator.writeStartArray();
        for (int i = 0; i < length; i++) {
            elementReader.readNextValueInto(generator);
        }
        generator.writeEndArray();
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
        long elementSkipSize = lengthStream.sum(skipSize);
        elementReader.skip(Ints.checkedCast(elementSkipSize));
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        lengthStream = null;

        elementReader.openStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        BooleanStreamSource presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        if (checkForNulls && presentStreamSource != null) {
            presentStream = presentStreamSource.openStream();
        }
        else {
            presentStream = null;
        }

        lengthStream = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStreamSource.class).openStream();

        elementReader.openRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
