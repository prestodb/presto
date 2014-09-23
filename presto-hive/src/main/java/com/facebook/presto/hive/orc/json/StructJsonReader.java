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
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.json.JsonReaders.createJsonReader;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class StructJsonReader
        implements JsonReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;

    private BooleanStream presentStream;

    private final String[] structFieldNames;
    private final JsonReader[] structFields;

    public StructJsonReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        List<StreamDescriptor> nestedStreams = streamDescriptor.getNestedStreams();
        this.structFieldNames = new String[nestedStreams.size()];
        this.structFields = new JsonReader[nestedStreams.size()];
        for (int i = 0; i < nestedStreams.size(); i++) {
            StreamDescriptor nestedStream = nestedStreams.get(i);
            this.structFields[i] = createJsonReader(nestedStream, true, sessionTimeZone);
            this.structFieldNames[i] = nestedStream.getFieldName();
        }
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
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
        BooleanStreamSource presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        if (checkForNulls && presentStreamSource != null) {
            presentStream = presentStreamSource.openStream();
        }
        else {
            presentStream = null;
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
