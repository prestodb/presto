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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DecodeTimestampOptions;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import jakarta.annotation.Nullable;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.ApacheHiveTimestampDecoder.decodeTimestamp;
import static com.facebook.presto.orc.reader.ReaderUtils.verifyStreamType;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getBooleanMissingStreamSource;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getLongMissingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TimestampBatchStreamReader
        implements BatchStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampBatchStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = getBooleanMissingStreamSource();
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> secondsStreamSource = getLongMissingStreamSource();
    @Nullable
    private LongInputStream secondsStream;

    private InputStreamSource<LongInputStream> nanosStreamSource = getLongMissingStreamSource();
    @Nullable
    private LongInputStream nanosStream;

    private boolean rowGroupOpen;
    private final boolean enableMicroPrecision;
    // When true, a timestamp that overflows the supported range is read back as null instead of
    // failing the read with a TimestampOutOfBoundsException.
    private final boolean readNullForOutOfBoundsTimestamp;
    private DecodeTimestampOptions decodeTimestampOptions;

    public TimestampBatchStreamReader(Type type, StreamDescriptor streamDescriptor, boolean enableMicroPrecision, boolean readNullForOutOfBoundsTimestamp)
            throws OrcCorruptionException
    {
        this.enableMicroPrecision = enableMicroPrecision;
        this.readNullForOutOfBoundsTimestamp = readNullForOutOfBoundsTimestamp;
        requireNonNull(type, "type is null");
        verifyStreamType(streamDescriptor, type, TimestampType.class::isInstance);
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock()
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
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is missing");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is missing");
                }

                secondsStream.skip(readOffset);
                nanosStream.skip(readOffset);
            }
        }

        Block block;
        if (secondsStream == null && nanosStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            block = RunLengthEncodedBlock.create(TIMESTAMP, null, nextBatchSize);
        }
        else if (presentStream == null) {
            block = readNonNullBlock();
        }
        else {
            boolean[] isNull = new boolean[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullCount == 0) {
                block = readNonNullBlock();
            }
            else if (nullCount != nextBatchSize) {
                block = readNullBlock(isNull);
            }
            else {
                block = RunLengthEncodedBlock.create(TIMESTAMP, null, nextBatchSize);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    private Block readNonNullBlock()
            throws IOException
    {
        if (secondsStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is missing");
        }
        if (nanosStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is missing");
        }

        return decodeBlock(null, nextBatchSize);
    }

    private Block readNullBlock(boolean[] isNull)
            throws IOException
    {
        if (secondsStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is missing");
        }
        if (nanosStream == null) {
            throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is missing");
        }

        return decodeBlock(isNull, isNull.length);
    }

    // Decodes positionCount values from the seconds/nanos streams, skipping the positions already
    // marked null in isNull. isNull is null for a column with no present stream; in that case it is
    // allocated by ensureIsNull on the first out-of-bounds timestamp, so a batch that decodes
    // cleanly still reports no nulls and keeps the downstream no-nulls fast path.
    private Block decodeBlock(boolean[] isNull, int positionCount)
            throws IOException
    {
        long[] values = new long[positionCount];
        boolean hasNull = isNull != null;

        for (int i = 0; i < positionCount; i++) {
            if (hasNull && isNull[i]) {
                continue;
            }
            if (decodeInto(values, i)) {
                // Out-of-bounds timestamps are read back as null instead of failing the read.
                isNull = ensureIsNull(isNull, positionCount);
                isNull[i] = true;
                hasNull = true;
            }
        }
        return new LongArrayBlock(positionCount, Optional.ofNullable(hasNull ? isNull : null), values);
    }

    // Returns the array the read loop marks out-of-bounds timestamps in, allocating it on first use.
    // The caller's array is returned unchanged when it is already present, so present-stream nulls
    // are never discarded.
    private static boolean[] ensureIsNull(boolean[] isNull, int capacity)
    {
        checkArgument(isNull == null || isNull.length >= capacity, "isNull is smaller than capacity");
        return isNull != null ? isNull : new boolean[capacity];
    }

    // Reads the next timestamp from the seconds/nanos streams into values[index]. Returns true when
    // the value is an out-of-bounds timestamp that should be read back as null; in that case
    // values[index] is left unset (0) and the caller must mark the position null. When
    // readNullForOutOfBoundsTimestamp is disabled the overflow is rethrown, preserving the original
    // fail-the-read behavior.
    private boolean decodeInto(long[] values, int index)
            throws IOException
    {
        long seconds = secondsStream.next();
        long serializedNanos = nanosStream.next();
        try {
            values[index] = decodeTimestamp(seconds, serializedNanos, decodeTimestampOptions);
            return false;
        }
        catch (TimestampOutOfBoundsException e) {
            if (!readNullForOutOfBoundsTimestamp) {
                throw e;
            }
            return true;
        }
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
    public void startStripe(ZoneId timezone, Stripe stripe)
    {
        decodeTimestampOptions = new DecodeTimestampOptions(timezone, enableMicroPrecision);

        presentStreamSource = getBooleanMissingStreamSource();
        secondsStreamSource = getLongMissingStreamSource();
        nanosStreamSource = getLongMissingStreamSource();

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        secondsStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);
        nanosStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);

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

    @Override
    public void close()
    {
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
