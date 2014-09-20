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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.orc.OrcCorruptionException.verifyFormat;
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
    private final boolean writeStackType;

    private final DateTimeFormatter timestampFormatter;

    private final long baseTimestampInSeconds;

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private LongStream secondsStream;

    @Nullable
    private LongStream nanosStream;

    public TimestampJsonReader(StreamDescriptor streamDescriptor, boolean writeStackType, DateTimeZone hiveStorageTimeZone, DateTimeZone sessionTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.writeStackType = writeStackType;
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

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        if (writeStackType) {
            generator.writeNumber(timestamp);
        }
        else {
            String formattedTimestamp = timestampFormatter.print(timestamp);
            generator.writeString(formattedTimestamp);
        }
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        if (writeStackType) {
            return String.valueOf(timestamp);
        }
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

        if (skipSize == 0) {
            return;
        }

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

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
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        secondsStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class).openStream();
        nanosStream = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class).openStream();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
