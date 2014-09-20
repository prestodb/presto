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
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.OrcCorruptionException.verifyFormat;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;

public class LongJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private LongStream dataStream;

    public LongJsonReader(StreamDescriptor streamDescriptor)
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

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

        generator.writeNumber(dataStream.next());
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

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

        if (skipSize == 0) {
            return;
        }

        verifyFormat(dataStream != null, "Value is not null but data stream is not present");

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
}
