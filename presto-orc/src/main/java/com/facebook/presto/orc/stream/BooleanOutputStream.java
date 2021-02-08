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

import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcOutputBuffer;
import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.ByteStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

public class BooleanOutputStream
        implements ValueOutputStream<BooleanStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanOutputStream.class).instanceSize();
    private final ByteOutputStream byteOutputStream;
    private final List<Integer> checkpointBitOffsets = new ArrayList<>();

    private int bitsInData;
    private int data;
    private boolean closed;

    public BooleanOutputStream(CompressionParameters compressionParameters, Optional<DwrfDataEncryptor> dwrfEncryptor)
    {
        this(new ByteOutputStream(compressionParameters, dwrfEncryptor));
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

        if (value) {
            data |= 0x1 << (7 - bitsInData);
        }
        bitsInData++;

        if (bitsInData == 8) {
            flushData();
        }
    }

    public void writeBooleans(int count, boolean value)
    {
        checkArgument(count >= 0, "count is negative");
        if (count == 0) {
            return;
        }

        if (bitsInData != 0) {
            int bitsToWrite = Math.min(count, 8 - bitsInData);
            if (value) {
                data |= getLowBitMask(bitsToWrite) << (8 - bitsInData - bitsToWrite);
            }

            bitsInData += bitsToWrite;
            count -= bitsToWrite;
            if (bitsInData == 8) {
                flushData();
            }
            else {
                // there were not enough bits to fill the current data
                verify(count == 0);
                return;
            }
        }

        // at this point there should be no pending data
        verify(bitsInData == 0);

        // write 8 bits at a time
        while (count >= 8) {
            if (value) {
                byteOutputStream.writeByte((byte) 0b1111_1111);
            }
            else {
                byteOutputStream.writeByte((byte) 0b0000_0000);
            }
            count -= 8;
        }

        // buffer remaining bits
        if (count > 0) {
            if (value) {
                data = getLowBitMask(count) << (8 - count);
            }
            bitsInData = count;
        }
    }

    private void flushData()
    {
        byteOutputStream.writeByte((byte) data);
        data = 0;
        bitsInData = 0;
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
            flushData();
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
    public StreamDataOutput getStreamDataOutput(int column)
    {
        checkState(closed);
        return byteOutputStream.getStreamDataOutput(column);
    }

    @Override
    public long getBufferedBytes()
    {
        return byteOutputStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + byteOutputStream.getRetainedBytes();
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

    private static int getLowBitMask(int bits)
    {
        return (0x1 << bits) - 1;
    }
}
