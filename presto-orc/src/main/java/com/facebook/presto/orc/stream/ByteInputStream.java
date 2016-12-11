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

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.checkpoint.ByteStreamCheckpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;
import static com.facebook.presto.orc.stream.OrcStreamUtils.readFully;

public class ByteInputStream
        implements ValueInputStream<ByteStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[MIN_REPEAT_SIZE + 127];
    private int length;
    private int offset;
    private long lastReadInputCheckpoint;

    public ByteInputStream(OrcInputStream input)
    {
        this.input = input;
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    // This is based on the Apache Hive ORC code
    private void readNextBlock()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException("Read past end of buffer RLE byte from %s", input);
        }

        offset = 0;

        // if byte high bit is not set, this is a repetition; otherwise it is a literal sequence
        if ((control & 0x80) == 0) {
            length = control + MIN_REPEAT_SIZE;

            // read the repeated value
            int value = input.read();
            if (value == -1) {
                throw new OrcCorruptionException("Reading RLE byte got EOF");
            }

            // fill buffer with the value
            Arrays.fill(buffer, 0, length, (byte) value);
        }
        else {
            // length is 2's complement of byte
            length = 0x100 - control;

            // read the literals into the buffer
            readFully(input, buffer, 0, length);
        }
    }

    @Override
    public Class<ByteStreamCheckpoint> getCheckpointType()
    {
        return ByteStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(ByteStreamCheckpoint checkpoint)
            throws IOException
    {
        // if the checkpoint is within the current buffer, just adjust the pointer
        if (lastReadInputCheckpoint == checkpoint.getInputStreamCheckpoint() && checkpoint.getOffset() <= length) {
            offset = checkpoint.getOffset();
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
            length = 0;
            offset = 0;
            skip(checkpoint.getOffset());
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (offset == length) {
                readNextBlock();
            }
            long consume = Math.min(items, length - offset);
            offset += consume;
            items -= consume;
        }
    }

    public byte next()
        throws IOException
    {
        if (offset == length) {
            readNextBlock();
        }
        return buffer[offset++];
    }

    public void nextVector(Type type, long items, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            type.writeLong(builder, next());
        }
    }

    public void nextVector(Type type, long items, BlockBuilder builder, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (isNull[i]) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, next());
            }
        }
    }
}
