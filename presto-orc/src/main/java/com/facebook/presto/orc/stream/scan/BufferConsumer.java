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
package com.facebook.presto.orc.stream.scan;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.stream.scan.OrcBufferIterator.Buffer;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.zigzagDecode;

class BufferConsumer
{
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;

    private final OrcBufferIterator input;
    private final boolean signed;

    private byte[] buffer;
    private int position;
    private int length;

    public BufferConsumer(OrcBufferIterator input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return input.getOrcDataSourceId();
    }

    public long getCheckpoint()
    {
        return input.getCheckpoint();
    }

    public int available()
    {
        return length - position;
    }

    public boolean refresh()
    {
        if (!input.hasNext()) {
            return false;
        }
        sync(input.next());
        return true;
    }

    private void sync(Buffer bufferContainer)
    {
        buffer = bufferContainer.getBuffer();
        position = bufferContainer.getPosition();
        length = bufferContainer.getLength();
    }

    public int read()
    {
        // If input has compressionKind == NONE then first bufferView will be empty
        if (available() == 0) {
            if (!refresh()) {
                return -1;
            }
        }
        return buffer[position++] & 0xff;
    }

    public long decodeVarint()
            throws IOException
    {
        long result = 0;
        int shift = 0;
        do {
            if (available() == 0) {
                if (!refresh() || available() == 0) {
                    throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
                }
            }
            result |= (long) (buffer[position] & 0x7f) << shift;
            shift += 7;
        }
        while ((buffer[position++] & 0x80) != 0);
        if (signed) {
            return zigzagDecode(result);
        }
        else {
            return result;
        }
    }

    public void skipFully(long items)
            throws IOException
    {
        if (items == 0) {
            return;
        }

        while (items > 0) {
            items -= skipVarintsInBuffer(items);
        }
    }

    private long skipVarintsInBuffer(long items)
            throws IOException
    {
        if (items == 0) {
            return 0;
        }
        if (available() == 0) {
            if (!refresh() || available() == 0) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "Unexpected EOF");
            }
        }
        long skipped = 0;
        long mask = 0;
        while (skipped < items && available() > 0) {
            if ((buffer[position++] & 0x80) == 0) {
                skipped++;
            }
        }
        /*
        while (skipped < items && available() >= SIZE_OF_LONG) {
            long value = ByteArrays.getLong(buffer, position);
            position += SIZE_OF_LONG;
            mask = (value & VARINT_MASK) ^ VARINT_MASK;
            skipped += Long.bitCount(mask);
        }
        if (skipped > items) {
            for (; skipped > items; skipped--) {
                mask ^= Long.highestOneBit(mask);
            }
            position -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else if (skipped == items) {
            position -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else {
            while (skipped < items && available() > 0) {
                if ((buffer[position++] & 0x80) == 0) {
                    skipped++;
                }
            }
        }*/
        return skipped;
    }

    public void seekToCheckpoint(long checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint);
        sync(input.peek());
    }
}
