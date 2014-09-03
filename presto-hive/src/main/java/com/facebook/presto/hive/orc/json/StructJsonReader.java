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
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import io.airlift.slice.DynamicSliceOutput;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.json.JsonReaders.createJsonReader;
import static com.facebook.presto.hive.orc.metadata.Stream.StreamKind.PRESENT;
import static com.google.common.base.Preconditions.checkNotNull;

public class StructJsonReader
        implements JsonReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean writeStackType;
    private final boolean checkForNulls;

    private final String[] structFieldNames;
    private final JsonReader[] structFields;

    private final DynamicSliceOutput buffer;

    @Nullable
    private BooleanStream presentStream;

    public StructJsonReader(StreamDescriptor streamDescriptor, boolean writeStackType, boolean checkForNulls, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.writeStackType = writeStackType;
        this.checkForNulls = checkForNulls;

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();
        this.structFieldNames = new String[nestedStreams.size()];
        this.structFields = new JsonReader[nestedStreams.size()];
        for (int i = 0; i < nestedStreams.size(); i++) {
            StreamDescriptor nestedStream = nestedStreams.get(i);
            this.structFields[i] = createJsonReader(nestedStream, true, false, hiveStorageTimeZone, sessionTimeZone);
            this.structFieldNames[i] = nestedStream.getFieldName();
        }

        buffer = new DynamicSliceOutput(1024);
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (writeStackType) {
            buffer.reset();
            try (JsonGenerator jsonGenerator = new JsonFactory().createGenerator(buffer)) {
                readNextValueIntoInternal(jsonGenerator);
            }

            byte[] byteArray = (byte[]) buffer.getUnderlyingSlice().getBase();
            generator.writeUTF8String(byteArray, 0, buffer.size());
        }
        else {
            readNextValueIntoInternal(generator);
        }
    }

    public void readNextValueIntoInternal(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        generator.writeStartObject();
        for (int structFieldIndex = 0; structFieldIndex < structFields.length; structFieldIndex++) {
            generator.writeFieldName(structFieldNames[structFieldIndex]);
            structFields[structFieldIndex].readNextValueInto(generator);
        }
        generator.writeEndObject();
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
        for (JsonReader structField : structFields) {
            structField.skip(skipSize);
        }
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;

        for (JsonReader structField : structFields) {
            structField.openStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        if (checkForNulls) {
            presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        }

        for (JsonReader structField : structFields) {
            structField.openRowGroup(dataStreamSources);
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
