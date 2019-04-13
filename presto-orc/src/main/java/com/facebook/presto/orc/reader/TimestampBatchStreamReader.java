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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.reader.ApacheHiveTimestampDecoder.decodeTimestamp;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class TimestampBatchStreamReader
        implements BatchStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampBatchStreamReader.class).instanceSize();

    private static final int MILLIS_PER_SECOND = 1000;

    private final StreamDescriptor streamDescriptor;
    private final long baseTimestampInSeconds;

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

    public TimestampBatchStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
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
    public Block readBlock(Type type)
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
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is not present");
                }

                secondsStream.skip(readOffset);
                nanosStream.skip(readOffset);
            }
        }

        if (secondsStream == null && nanosStream == null && presentStream != null) {
            presentStream.skip(nextBatchSize);
            Block nullValueBlock = RunLengthEncodedBlock.create(type, null, nextBatchSize);
            readOffset = 0;
            nextBatchSize = 0;
            return nullValueBlock;
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);

        if (presentStream == null) {
            if (secondsStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is not present");
            }
            if (nanosStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is not present");
            }

            for (int i = 0; i < nextBatchSize; i++) {
                type.writeLong(builder, decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds));
            }
        }
        else {
            verify(secondsStream != null, "Value is not null but seconds stream is not present");
            verify(nanosStream != null, "Value is not null but nanos stream is not present");
            for (int i = 0; i < nextBatchSize; i++) {
                if (presentStream.nextBit()) {
                    type.writeLong(builder, decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds));
                }
                else {
                    builder.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return builder.build();
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
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
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
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
