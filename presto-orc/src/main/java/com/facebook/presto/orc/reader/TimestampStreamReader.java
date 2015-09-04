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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Vector;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.LongStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.OrcReaderUtils.castOrcVector;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TimestampStreamReader
        implements StreamReader
{
    private static final int MILLIS_PER_SECOND = 1000;

    private final StreamDescriptor streamDescriptor;
    private final long baseTimestampInSeconds;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<LongStream> secondsStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream secondsStream;

    @Nonnull
    private StreamSource<LongStream> nanosStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream nanosStream;

    private final long[] nanosVector = new long[Vector.MAX_VECTOR_LENGTH];

    private boolean rowGroupOpen;

    public TimestampStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (secondsStream == null) {
                    throw new OrcCorruptionException("Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException("Value is not null but nanos stream is not present");
                }

                secondsStream.skip(readOffset);
                nanosStream.skip(readOffset);
            }
        }

        LongVector longVector = castOrcVector(vector, LongVector.class);
        if (presentStream == null) {
            if (secondsStream == null) {
                throw new OrcCorruptionException("Value is not null but seconds stream is not present");
            }
            if (nanosStream == null) {
                throw new OrcCorruptionException("Value is not null but nanos stream is not present");
            }

            Arrays.fill(longVector.isNull, false);
            secondsStream.nextLongVector(nextBatchSize, longVector.vector);
            nanosStream.nextLongVector(nextBatchSize, nanosVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, longVector.isNull);
            if (nullValues != nextBatchSize) {
                if (secondsStream == null) {
                    throw new OrcCorruptionException("Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException("Value is not null but nanos stream is not present");
                }

                secondsStream.nextLongVector(nextBatchSize, longVector.vector, longVector.isNull);
                nanosStream.nextLongVector(nextBatchSize, nanosVector, longVector.isNull);
            }
        }

        // merge seconds and nanos together
        for (int i = 0; i < nextBatchSize; i++) {
            longVector.vector[i] = decodeTimestamp(longVector.vector[i], nanosVector[i], baseTimestampInSeconds);
        }

        readOffset = 0;
        nextBatchSize = 0;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        secondsStream = secondsStreamSource.openStream();
        nanosStream = nanosStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        secondsStreamSource = missingStreamSource(LongStream.class);
        nanosStreamSource = missingStreamSource(LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        secondsStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class);
        nanosStreamSource = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    // This comes from the Apache Hive ORC code
    public static long decodeTimestamp(long seconds, long serializedNanos, long baseTimestampInSeconds)
    {
        long millis = (seconds + baseTimestampInSeconds) * MILLIS_PER_SECOND;
        long nanos = parseNanos(serializedNanos);

        // the rounding error exists because java always rounds up when dividing integers
        // -42001/1000 = -42; and -42001 % 1000 = -1 (+ 1000)
        // to get the correct value we need
        // (-42 - 1)*1000 + 999 = -42001
        // (42)*1000 + 1 = 42001
        if (millis < 0 && nanos != 0) {
            millis -= 1000;
        }
        // Truncate nanos to millis and add to mills
        return millis + (nanos / 1_000_000);
    }

    // This comes from the Apache Hive ORC code
    private static int parseNanos(long serialized)
    {
        int zeros = ((int) serialized) & 0b111;
        int result = (int) (serialized >>> 3);
        if (zeros != 0) {
            for (int i = 0; i <= zeros; ++i) {
                result *= 10;
            }
        }
        return result;
    }
}
