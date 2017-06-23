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
import com.facebook.presto.orc.checkpoint.ByteStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.google.common.base.Preconditions.checkState;

public class ByteOutputStream
        implements ValueOutputStream<ByteStreamCheckpoint>
{
    // todo use OrcStreamUtils
    private static final int MIN_REPEAT_SIZE = 3;
    private static final int UNMATCHABLE_VALUE = Integer.MAX_VALUE;

    private final OrcOutputBuffer buffer;
    private final List<ByteStreamCheckpoint> checkpoints = new ArrayList<>();

    private final byte[] sequenceBuffer = new byte[128];
    private int size;

    private int runCount;
    private int lastValue = UNMATCHABLE_VALUE;

    private boolean closed;

    public ByteOutputStream(CompressionKind compression, int bufferSize)
    {
        this(new OrcOutputBuffer(compression, bufferSize));
    }

    public ByteOutputStream(OrcOutputBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public Class<ByteStreamCheckpoint> getCheckpointType()
    {
        return ByteStreamCheckpoint.class;
    }

    public void writeByte(byte value)
    {
        checkState(!closed);

        // flush if buffer is full
        if (size == sequenceBuffer.length) {
            flushSequence();
        }

        // update run count
        if (value == lastValue) {
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
        // this
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

        lastValue = value;
    }

    private void flushSequence()
    {
        if (size == 0) {
            return;
        }

        if (runCount >= MIN_REPEAT_SIZE) {
            buffer.writeByte(runCount - MIN_REPEAT_SIZE);
            buffer.writeByte(lastValue);
        }
        else {
            buffer.writeByte(-size);
            for (int i = 0; i < size; i++) {
                buffer.writeByte(sequenceBuffer[i]);
            }
        }

        size = 0;
        runCount = 0;
        lastValue = UNMATCHABLE_VALUE;
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new ByteStreamCheckpoint(size, buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        flushSequence();
    }

    @Override
    public List<ByteStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        int length = buffer.writeDataTo(outputStream);
        return Optional.of(new Stream(column, DATA, length, false));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.size() + size;
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
        lastValue = UNMATCHABLE_VALUE;

        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}
