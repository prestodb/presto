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
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.ByteStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class BooleanOutputStream
        implements ValueOutputStream<BooleanStreamCheckpoint>
{
    private final ByteOutputStream byteOutputStream;
    private final List<Integer> checkpointBitOffsets = new ArrayList<>();

    private int bitsInData;
    private int data;
    private boolean closed;

    public BooleanOutputStream(CompressionKind compression, int bufferSize)
    {
        this(new ByteOutputStream(compression, bufferSize));
    }

    public BooleanOutputStream(OrcOutputBuffer buffer)
    {
        this(new ByteOutputStream(buffer));
    }

    public BooleanOutputStream(ByteOutputStream byteOutputStream)
    {
        this.byteOutputStream = byteOutputStream;
    }

    public void writeBoolean(boolean value)
    {
        checkState(!closed);

        data <<= 1;
        if (value) {
            data |= 0x1;
        }
        bitsInData++;

        if (bitsInData == 8) {
            byteOutputStream.writeByte((byte) data);
            data = 0;
            bitsInData = 0;
        }
    }

    public void writeBooleans(int count, boolean value)
    {
        // todo write whole bytes
        for (int i = 0; i < count; i++) {
            writeBoolean(value);
        }
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        byteOutputStream.recordCheckpoint();
        checkpointBitOffsets.add(bitsInData);
    }

    @Override
    public void close()
    {
        closed = true;
        if (bitsInData > 0) {
            data <<= (8 - bitsInData);
            byteOutputStream.writeByte((byte) data);
            data = 0;
            bitsInData = 0;
        }
        byteOutputStream.close();
    }

    @Override
    public List<BooleanStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        ImmutableList.Builder<BooleanStreamCheckpoint> booleanStreamCheckpoint = ImmutableList.builder();
        List<ByteStreamCheckpoint> byteStreamCheckpoints = byteOutputStream.getCheckpoints();
        for (int groupId = 0; groupId < checkpointBitOffsets.size(); groupId++) {
            int checkpointBitOffset = checkpointBitOffsets.get(groupId);
            ByteStreamCheckpoint byteStreamCheckpoint = byteStreamCheckpoints.get(groupId);
            booleanStreamCheckpoint.add(new BooleanStreamCheckpoint(checkpointBitOffset, byteStreamCheckpoint));
        }
        return booleanStreamCheckpoint.build();
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        return byteOutputStream.writeDataStreams(column, outputStream);
    }

    @Override
    public long getBufferedBytes()
    {
        return byteOutputStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        return byteOutputStream.getRetainedBytes();
    }

    @Override
    public void reset()
    {
        data = 0;
        bitsInData = 0;

        closed = false;
        byteOutputStream.reset();
        checkpointBitOffsets.clear();
    }
}
