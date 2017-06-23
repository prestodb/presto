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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcOutputBuffer;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamV1Checkpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LongOutputStreamV1
        implements LongOutputStream
{
    // todo use OrcStreamUtils
    private static final int MIN_REPEAT_SIZE = 3;
    private static final long UNMATCHABLE_DELTA_VALUE = Long.MAX_VALUE;
    private static final int MAX_DELTA = 127;
    private static final int MIN_DELTA = -128;

    private final StreamKind streamKind;
    private final OrcOutputBuffer buffer;
    private final List<LongStreamCheckpoint> checkpoints = new ArrayList<>();

    private final long[] sequenceBuffer = new long[128];
    private final boolean signed;
    private int size;

    private int runCount;
    private long lastValue;
    private long lastDelta = UNMATCHABLE_DELTA_VALUE;

    private boolean closed;

    public LongOutputStreamV1(CompressionKind compression, int bufferSize, boolean signed, StreamKind streamKind)
    {
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
        this.signed = signed;
    }

    @Override
    public void writeLong(long value)
    {
        checkState(!closed);

        // flush if buffer is full
        if (size == sequenceBuffer.length) {
            flushSequence();
        }

        // update run count
        long delta = value - lastValue;
        if (isValidDelta(delta) && value == lastValue) {
            runCount++;
        }
        else {
            // run ended, so flush
            if (runCount >= MIN_REPEAT_SIZE) {
                flushSequence();
            }
            runCount = 1;
        }

        // buffer value
        sequenceBuffer[size] = value;
        size++;

        // before buffering value, check if a run started, and if so flush buffered literal values
        if (runCount == MIN_REPEAT_SIZE && size > MIN_REPEAT_SIZE) {
            // flush the sequence up to the beginning of the run, which must be MIN_REPEAT_SIZE
            size -= MIN_REPEAT_SIZE;
            runCount = 0;

            flushSequence();

            // reset the runCount to the MIN_REPEAT_SIZE
            runCount = MIN_REPEAT_SIZE;
            size = MIN_REPEAT_SIZE;

            // note there is no reason to add the run values to the buffer since is is not used
            // when in a run length sequence
        }

        lastDelta = delta;
        lastValue = value;
    }

    private static boolean isValidDelta(long delta)
    {
        return delta >= MIN_DELTA && delta <= MAX_DELTA;
    }

    private void flushSequence()
    {
        if (size == 0) {
            return;
        }

        if (runCount >= MIN_REPEAT_SIZE) {
            buffer.writeByte(runCount - MIN_REPEAT_SIZE);
            buffer.writeByte((byte) lastDelta);
            writeVintToBuffer(lastValue);
        }
        else {
            buffer.writeByte(-size);
            for (int i = 0; i < size; i++) {
                writeVintToBuffer(sequenceBuffer[i]);
            }
        }

        size = 0;
        runCount = 0;
        lastValue = UNMATCHABLE_DELTA_VALUE; // todo should this be last delta?
    }

    private void writeVintToBuffer(long value)
    {
        if (signed) {
            value = (value << 1) ^ (value >> 63);
        }
        writeVLong(buffer, value);
    }

    // todo see if this can be faster
    public static void writeVLong(SliceOutput output, long value)
    {
        while (true) {
            // if there are less than 7 bits left, we are done
            if ((value & ~0b111_1111) == 0) {
                output.write((byte) value);
                return;
            }
            else {
                output.write((byte) (0x80 | (value & 0x7f)));
                value >>>= 7;
            }
        }
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new LongStreamV1Checkpoint(size, buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        flushSequence();
    }

    @Override
    public List<LongStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        int length = buffer.writeDataTo(outputStream);
        return Optional.of(new Stream(column, streamKind, length, true));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.size() + (Long.BYTES * size);
    }

    @Override
    public long getRetainedBytes()
    {
        return buffer.getRetainedSize() + SizeOf.sizeOf(sequenceBuffer);
    }

    @Override
    public void reset()
    {
        size = 0;
        runCount = 0;
        lastValue = 0;
        lastDelta = UNMATCHABLE_DELTA_VALUE;

        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}
