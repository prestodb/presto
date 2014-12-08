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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.primitives.Ints;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.json.JsonReaders.createJsonReader;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class ListJsonReader
        implements JsonReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;

    private final JsonReader elementReader;

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private LongStream lengthStream;

    public ListJsonReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        elementReader = createJsonReader(streamDescriptor.getNestedStreams().get(0), true, hiveStorageTimeZone);
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
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

        if (skipSize == 0) {
            return;
        }

        if (lengthStream == null) {
            throw new OrcCorruptionException("Value is not null but length stream is not present");
        }

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
        if (checkForNulls) {
            presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        }
        lengthStream = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();

        elementReader.openRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
