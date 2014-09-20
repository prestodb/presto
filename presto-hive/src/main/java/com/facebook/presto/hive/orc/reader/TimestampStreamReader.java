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
package com.facebook.presto.hive.orc.reader;

import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.StreamDescriptor;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.hive.orc.stream.BooleanStream;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.LongStream;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSources;
import com.google.common.base.Objects;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.SECONDARY;

public class TimestampStreamReader
        implements StreamReader
{
    private static final int MILLIS_PER_SECOND = 1000;

    private final StreamDescriptor streamDescriptor;
    private final long baseTimestampInSeconds;

    private int skipSize;
    private int nextBatchSize;

    private BooleanStreamSource presentStreamSource;
    private BooleanStream presentStream;

    private LongStreamSource secondsStreamSource;
    private LongStream secondsStream;

    private LongStreamSource nanosStreamSource;
    private LongStream nanosStream;
    private final long[] nanosVector = new long[Vector.MAX_VECTOR_LENGTH];

    public TimestampStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
    }

    @Override
    public void setNextBatchSize(int batchSize)
    {
        skipSize += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (secondsStream == null) {
            openStreams();
        }

        if (skipSize != 0) {
            if (presentStreamSource != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                skipSize = presentStream.countBitsSet(skipSize);
            }
            secondsStream.skip(skipSize);
            nanosStream.skip(skipSize);
        }

        LongVector longVector = (LongVector) vector;
        if (presentStream == null) {
            Arrays.fill(longVector.isNull, false);
            secondsStream.nextLongVector(nextBatchSize, longVector.vector);
            nanosStream.nextLongVector(nextBatchSize, nanosVector);
        }
        else {
            presentStream.getUnsetBits(nextBatchSize, longVector.isNull);
            secondsStream.nextLongVector(nextBatchSize, longVector.vector, longVector.isNull);
            nanosStream.nextLongVector(nextBatchSize, nanosVector, longVector.isNull);
        }

        // merge seconds and nanos together
        for (int i = 0; i < nextBatchSize; i++) {
            longVector.vector[i] = decodeTimestamp(longVector.vector[i], nanosVector[i], baseTimestampInSeconds);
        }

        skipSize = 0;
        nextBatchSize = 0;
    }

    public static long decodeTimestamp(long seconds, long serializedNanos, long baseTimestampInSeconds)
    {
        long ms = (seconds + baseTimestampInSeconds) * MILLIS_PER_SECOND;
        long ns = parseNanos(serializedNanos);
        // the rounding error exists because java always rounds up when dividing integers
        // -42001/1000 = -42; and -42001 % 1000 = -1 (+ 1000)
        // to get the correct value we need
        // (-42 - 1)*1000 + 999 = -42001
        // (42)*1000 + 1 = 42001
        if (ms < 0 && ns != 0) {
            ms -= 1000;
        }
        // Convert millis into nanos and add the nano vector value to it
        return ms + (ns / 1_000_000);
    }

    private void openStreams()
            throws IOException
    {
        if (presentStreamSource != null) {
            if (presentStream == null) {
                presentStream = presentStreamSource.openStream();
            }
        }

        secondsStream = secondsStreamSource.openStream();
        nanosStream = nanosStreamSource.openStream();
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = null;
        secondsStreamSource = null;
        nanosStreamSource = null;

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSourceIfPresent(streamDescriptor, PRESENT, BooleanStreamSource.class);
        secondsStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStreamSource.class);
        nanosStreamSource = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStreamSource.class);

        skipSize = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private static int parseNanos(long serialized)
    {
        int zeros = 7 & (int) serialized;
        int result = (int) (serialized >>> 3);
        if (zeros != 0) {
            for (int i = 0; i <= zeros; ++i) {
                result *= 10;
            }
        }
        return result;
    }
}
