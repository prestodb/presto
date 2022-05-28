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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.LongOutputStream;
import com.facebook.presto.orc.stream.LongOutputStreamV1;
import com.facebook.presto.orc.stream.LongOutputStreamV2;
import com.facebook.presto.orc.stream.PresentOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.writer.ColumnWriterUtils.buildRowGroupIndexes;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TimestampColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampColumnWriter.class).instanceSize();
    private static final int MILLIS_PER_SECOND = 1000;
    private static final int MICROS_PER_SECOND = 1000_000;
    private static final int MILLIS_TO_NANOS_TRAILING_ZEROS = 5;
    private static final int MICROS_TO_NANOS_TRAILING_ZEROS = 2;
    // Timestamp is encoded as Seconds (Long) and Nanos (Integer)
    private static final long TIMESTAMP_RAW_SIZE = Long.BYTES + Integer.BYTES;

    private final int column;
    private final int sequence;
    private final Type type;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream secondsStream;
    private final LongOutputStream nanosStream;
    private final PresentOutputStream presentStream;
    private final CompressedMetadataWriter metadataWriter;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private long columnStatisticsRetainedSizeInBytes;

    private final long baseTimestampInSeconds;
    private final int unitsPerSecond;
    private final int trailingZeros;

    private int nonNullValueCount;

    private boolean closed;

    public TimestampColumnWriter(
            int column,
            int sequence,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor,
            OrcEncoding orcEncoding,
            DateTimeZone hiveStorageTimeZone,
            MetadataWriter metadataWriter)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(sequence >= 0, "sequence is negative");
        requireNonNull(columnWriterOptions, "compression is null");
        requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        requireNonNull(metadataWriter, "metadataWriter is null");
        this.column = column;
        this.sequence = sequence;
        this.type = requireNonNull(type, "type is null");
        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        if (type == TIMESTAMP) {
            this.unitsPerSecond = MILLIS_PER_SECOND;
            this.trailingZeros = MILLIS_TO_NANOS_TRAILING_ZEROS;
        }
        else if (type == TIMESTAMP_MICROSECONDS) {
            this.unitsPerSecond = MICROS_PER_SECOND;
            this.trailingZeros = MICROS_TO_NANOS_TRAILING_ZEROS;
        }
        else {
            throw new UnsupportedOperationException("Unsupported Type: " + type);
        }
        if (orcEncoding == DWRF) {
            this.columnEncoding = new ColumnEncoding(DIRECT, 0);
            this.secondsStream = new LongOutputStreamV1(columnWriterOptions, dwrfEncryptor, true, DATA);
            this.nanosStream = new LongOutputStreamV1(columnWriterOptions, dwrfEncryptor, false, SECONDARY);
        }
        else {
            this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
            this.secondsStream = new LongOutputStreamV2(columnWriterOptions, true, DATA);
            this.nanosStream = new LongOutputStreamV2(columnWriterOptions, false, SECONDARY);
        }
        this.presentStream = new PresentOutputStream(columnWriterOptions, dwrfEncryptor);
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        secondsStream.recordCheckpoint();
        nanosStream.recordCheckpoint();
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        int blockNonNullValueCount = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                long value = type.getLong(block, position);

                // It is a flaw in ORC encoding that uses normal integer division to compute seconds,
                // and floor modulus to compute nano seconds.
                long seconds = (value / unitsPerSecond) - baseTimestampInSeconds;
                long subSecondValue = Math.floorMod(value, unitsPerSecond);
                // The "sub-second" value (i.e., the nanos value) typically has a large number of trailing
                // zero, because many systems, like Presto, only record millisecond or microsecond precision
                // timestamps. To optimize storage, if the value has more than two trailing zeros, the trailing
                // decimal zero digits are removed, and the last three bits are used to record how many zeros
                // were removed (minus one):
                //   # Trailing 0s   Last 3 Bits   Example nanos       Example encoding
                //         0            0b000        123456789     (123456789 << 3) | 0b000
                //         1            0b000        123456780     (123456780 << 3) | 0b000
                //         2            0b001        123456700       (1234567 << 3) | 0b001
                //         3            0b010        123456000        (123456 << 3) | 0b010
                //         4            0b011        123450000         (12345 << 3) | 0b011
                //         5            0b100        123400000          (1234 << 3) | 0b100
                //         6            0b101        123000000           (123 << 3) | 0b101
                //         7            0b110        120000000            (12 << 3) | 0b110
                //         8            0b111        100000000             (1 << 3) | 0b111
                //
                // Presto-orc supports millisecond or microsecond precision.
                // Except when input is zero, we use the encoding for 6 trailing zeros (millisecond precision) or 3 trailing zeros (microsecond precision)
                // For simplicity, we don't dynamically use 3 to 8 zeros depending on the circumstance.
                long encodedNanos = subSecondValue == 0 ? 0 : (subSecondValue << 3) | trailingZeros;

                secondsStream.writeLong(seconds);
                nanosStream.writeLong(encodedNanos);
                blockNonNullValueCount++;
            }
        }

        nonNullValueCount += blockNonNullValueCount;
        return (block.getPositionCount() - blockNonNullValueCount) * NULL_SIZE + blockNonNullValueCount * TIMESTAMP_RAW_SIZE;
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = new ColumnStatistics((long) nonNullValueCount, null);
        rowGroupColumnStatistics.add(statistics);
        columnStatisticsRetainedSizeInBytes += statistics.getRetainedSizeInBytes();
        nonNullValueCount = 0;
        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        secondsStream.close();
        nanosStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<? extends StreamCheckpoint>> prependCheckpoints)
            throws IOException
    {
        checkState(closed);

        List<RowGroupIndex> rowGroupIndexes = buildRowGroupIndexes(compressed, rowGroupColumnStatistics, prependCheckpoints, presentStream, secondsStream, nanosStream);
        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes);
        Stream stream = new Stream(column, sequence, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(column, sequence).ifPresent(outputDataStreams::add);
        outputDataStreams.add(secondsStream.getStreamDataOutput(column, sequence));
        outputDataStreams.add(nanosStream.getStreamDataOutput(column, sequence));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return secondsStream.getBufferedBytes() + nanosStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + secondsStream.getRetainedBytes() + nanosStream.getRetainedBytes() + presentStream.getRetainedBytes() + columnStatisticsRetainedSizeInBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        secondsStream.reset();
        nanosStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        columnStatisticsRetainedSizeInBytes = 0;
        nonNullValueCount = 0;
    }
}
