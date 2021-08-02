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
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.ApacheHiveTimestampDecoder.decodeTimestamp;
import static com.facebook.presto.orc.reader.ReaderUtils.verifyStreamType;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TimestampBatchStreamReader
        implements BatchStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampBatchStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> secondsStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream secondsStream;

    private InputStreamSource<LongInputStream> nanosStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream nanosStream;

    private boolean rowGroupOpen;
    private final DecodeTimestampOptions decodeTimestampOptions;

    public TimestampBatchStreamReader(Type type, StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, OrcRecordReaderOptions decodeTimestampOptions)
            throws OrcCorruptionException
    {
        this.decodeTimestampOptions = new DecodeTimestampOptions(hiveStorageTimeZone, decodeTimestampOptions.enableTimestampMicroPrecision());
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

        long[] values = new long[nextBatchSize];
        for (int i = 0; i < nextBatchSize; i++) {
            values[i] = decodeTimestamp(secondsStream.next(), nanosStream.next(), decodeTimestampOptions);
        }
        return new LongArrayBlock(nextBatchSize, Optional.empty(), values);
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

        long[] values = new long[isNull.length];

        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                values[i] = decodeTimestamp(secondsStream.next(), nanosStream.next(), decodeTimestampOptions);
            }
        }
        return new LongArrayBlock(isNull.length, Optional.of(isNull), values);
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
    public void startStripe(Stripe stripe)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        secondsStreamSource = missingStreamSource(LongInputStream.class);
        nanosStreamSource = missingStreamSource(LongInputStream.class);

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
