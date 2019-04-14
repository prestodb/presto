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

import java.io.IOException;
import java.util.Arrays;

import static java.lang.Math.min;

public class ByteInputStream
        implements ValueInputStream<ByteStreamCheckpoint>
{
    private static final int MIN_REPEAT_SIZE = 3;

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
            throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of buffer RLE byte");
        }

        offset = 0;

        // if byte high bit is not set, this is a repetition; otherwise it is a literal sequence
        if ((control & 0x80) == 0) {
            length = control + MIN_REPEAT_SIZE;

            // read the repeated value
            int value = input.read();
            if (value == -1) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "Reading RLE byte got EOF");
            }

            // fill buffer with the value
            Arrays.fill(buffer, 0, length, (byte) value);
        }
        else {
            // length is 2's complement of byte
            length = 0x100 - control;

            // read the literals into the buffer
            input.readFully(buffer, 0, length);
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
            long consume = min(items, length - offset);
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

    public byte[] next(int items)
            throws IOException
    {
        byte[] values = new byte[items];
        next(values, items);
        return values;
    }

    public void next(byte[] values, int items)
            throws IOException
    {
        int outputOffset = 0;
        while (outputOffset < items) {
            if (offset == length) {
                readNextBlock();
            }
            if (length == 0) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "Unexpected end of stream");
            }

            int chunkSize = min(items - outputOffset, length - offset);
            System.arraycopy(buffer, offset, values, outputOffset, chunkSize);

            outputOffset += chunkSize;
            offset += chunkSize;
        }
    }
}
