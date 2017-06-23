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
import com.facebook.presto.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LongOutputStreamDwrf
        implements LongOutputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongOutputStreamDwrf.class).instanceSize();
    private final StreamKind streamKind;
    private final OrcOutputBuffer buffer;
    private final boolean signed;
    private final List<LongStreamDwrfCheckpoint> checkpoints = new ArrayList<>();

    private boolean closed;

    public LongOutputStreamDwrf(CompressionKind compression, int bufferSize, boolean signed, StreamKind streamKind)
    {
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
        this.signed = signed;
    }

    @Override
    public void writeLong(long value)
    {
        checkState(!closed);

        if (signed) {
            value = (value << 1) ^ (value >> 63);
        }
        writeVLong(buffer, value);
    }

    // todo see if this can be faster
    private static void writeVLong(SliceOutput output, long value)
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
        checkpoints.add(new LongStreamDwrfCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
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
        return buffer.size();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize();
    }

    @Override
    public void reset()
    {
        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}
