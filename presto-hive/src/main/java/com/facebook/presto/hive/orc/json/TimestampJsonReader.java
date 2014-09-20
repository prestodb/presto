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
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.reader.TimestampStreamReader.decodeTimestamp;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.SECONDARY;

public class TimestampJsonReader
        implements JsonMapKeyReader
{
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final StreamDescriptor streamDescriptor;

    private final DateTimeFormatter timestampFormatter;

    private final long baseTimestampInSeconds;

    private BooleanStream presentStream;
    private LongStream secondsStream;
    private LongStream nanosStream;

    public TimestampJsonReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.timestampFormatter = TIMESTAMP_FORMATTER.withZone(sessionTimeZone);
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / 1000;
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        String formattedTimestamp = timestampFormatter.print(timestamp);
        generator.writeString(formattedTimestamp);
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        return timestampFormatter.print(timestamp);
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
        secondsStream.skip(skipSize);
        nanosStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        secondsStream = null;
        nanosStream = null;
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

        secondsStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStreamSource.class).openStream();
        nanosStream = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStreamSource.class).openStream();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
