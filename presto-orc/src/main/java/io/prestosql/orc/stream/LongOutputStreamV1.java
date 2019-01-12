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
package io.prestosql.orc.stream;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.prestosql.orc.OrcOutputBuffer;
import io.prestosql.orc.checkpoint.LongStreamCheckpoint;
import io.prestosql.orc.checkpoint.LongStreamV1Checkpoint;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.Stream.StreamKind;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.orc.stream.LongDecode.writeVLong;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class LongOutputStreamV1
        implements LongOutputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongOutputStreamV1.class).instanceSize();
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

        // buffer value
        sequenceBuffer[size] = value;
        size++;

        // update run count
        long delta = value - lastValue;
        if (delta == lastDelta) {
            // extend the run
            runCount++;

            // if a run started, flush buffered literal values
            if (runCount == MIN_REPEAT_SIZE && size > MIN_REPEAT_SIZE) {
                // flush the literal sequence up to the beginning of the run, which must be MIN_REPEAT_SIZE
                flushLiteralSequence(size - MIN_REPEAT_SIZE);

                size = MIN_REPEAT_SIZE;

                // note there is no reason to add the run values to the buffer since it is not used
                // when in a run length sequence
            }
        }
        else {
            // if a run ended, flush rle
            if (runCount >= MIN_REPEAT_SIZE) {
                flushRleSequence(runCount);

                // the current value is the first literal of a new sequence
                sequenceBuffer[0] = value;
                size = 1;
            }

            if (size == 1 || !isValidDelta(delta)) {
                // the run starts with this value
                runCount = 1;
                lastDelta = UNMATCHABLE_DELTA_VALUE;
            }
            else {
                // this is a new run formed from the last value and the current value
                runCount = 2;
                lastDelta = delta;
            }
        }

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
            flushRleSequence(runCount);
        }
        else {
            flushLiteralSequence(size);
        }

        size = 0;
        runCount = 0;
        lastValue = 0;
        lastDelta = UNMATCHABLE_DELTA_VALUE;
    }

    private void flushLiteralSequence(int literalCount)
    {
        verify(literalCount > 0);

        buffer.writeByte(-literalCount);
        for (int i = 0; i < literalCount; i++) {
            writeVLong(buffer, sequenceBuffer[i], signed);
        }
    }

    private void flushRleSequence(int runCount)
    {
        verify(runCount > 0);

        buffer.writeByte(runCount - MIN_REPEAT_SIZE);
        buffer.writeByte((byte) lastDelta);

        // write the start value of the sequence
        long totalDeltaSize = lastDelta * (this.runCount - 1);
        long sequenceStartValue = lastValue - totalDeltaSize;
        writeVLong(buffer, sequenceStartValue, signed);
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
        buffer.close();
    }

    @Override
    public List<LongStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public StreamDataOutput getStreamDataOutput(int column)
    {
        return new StreamDataOutput(buffer::writeDataTo, new Stream(column, streamKind, toIntExact(buffer.getOutputDataSize()), true));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.estimateOutputDataSize() + (Long.BYTES * size);
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize() + SizeOf.sizeOf(sequenceBuffer);
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
