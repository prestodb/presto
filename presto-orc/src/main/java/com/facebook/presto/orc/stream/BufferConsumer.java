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
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import io.airlift.slice.ByteArrays;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.orc.stream.LongDecode.zigzagDecode;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BufferConsumer
{
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final int MAX_VARINT_LENGTH = 10;

    private final OrcInputStream input;
    private final boolean signed;
    private final Optional<OrcTypeKind> orcTypeKind;

    private byte[] buffer;
    private int position;
    private int length;

    public BufferConsumer(OrcInputStream input, boolean signed, Optional<OrcTypeKind> orcTypeKind)
    {
        this.input = input;
        this.signed = signed;
        this.orcTypeKind = requireNonNull(orcTypeKind, "orcTypeKind is null");
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

    public long readFixedWidthType()
    {
        checkState(orcTypeKind.isPresent(), "orcTypeKind must not be empty");
        switch (orcTypeKind.get()) {
            case SHORT:
                return read() | (read() << 8);
            case INT:
                if (available() >= SIZE_OF_INT) {
                    long result = ByteArrays.getInt(buffer, position);
                    position += SIZE_OF_INT;
                    return result;
                }
                return read() | (read() << 8) | (read() << 16) | (read() << 24);
            case LONG:
                if (available() >= SIZE_OF_LONG) {
                    long result = ByteArrays.getLong(buffer, position);
                    position += SIZE_OF_LONG;
                    return result;
                }
                return ((long) read()) |
                        (((long) read()) << 8) |
                        (((long) read()) << 16) |
                        (((long) read()) << 24) |
                        (((long) read()) << 32) |
                        (((long) read()) << 40) |
                        (((long) read()) << 48) |
                        (((long) read()) << 56);
            default:
                throw new IllegalStateException();
        }
    }

    public void skipFixedWidthType(long items)
            throws IOException
    {
        checkState(orcTypeKind.isPresent(), "orcTypeKind must not be empty");
        if (items == 0) {
            return;
        }
        long bytes = items;
        switch (orcTypeKind.get()) {
            case SHORT:
                bytes *= SIZE_OF_SHORT;
                break;
            case INT:
                bytes *= SIZE_OF_INT;
                break;
            case LONG:
                bytes *= SIZE_OF_LONG;
                break;
            default:
                throw new IllegalStateException();
        }
        skipBytes(bytes);
    }

    public long decodeVarint()
            throws IOException
    {
        long result = 0;
        int shift = 0;
        int available = available();
        if (available >= 2 * Long.BYTES) {
            long word = ByteArrays.getLong(buffer, position);
            int count = 1;
            result = word & 0x7f;
            if ((word & 0x80) != 0) {
                long control = word >>> 8;
                long mask = 0x7f << 7;
                while (true) {
                    word = word >>> 1;
                    result |= word & mask;
                    count++;
                    if ((control & 0x80) == 0) {
                        break;
                    }
                    mask = mask << 7;
                    control = control >>> 8;
                }
            }
            if (count == 8) {
                word = ByteArrays.getLong(buffer, position + 8);
                result |= (word & 0x7f) << 56;
                if ((word & 0x80) == 0) {
                    count++;
                }
                else {
                    result |= 1L << 63;
                    count += 2;
                }
            }
            position += count;
        }
        else {
            byte currentByte;
            do {
                if (available == 0) {
                    if (!refresh() || available() == 0) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
                    }
                    available = available();
                }
                currentByte = buffer[position];
                result |= (long) (currentByte & 0x7f) << shift;
                shift += 7;
                available--;
                position++;
            }
            while ((currentByte & 0x80) != 0);
        }
        if (signed) {
            return zigzagDecode(result);
        }
        else {
            return result;
        }
    }

    private void skipBytes(long bytes)
            throws IOException
    {
        if (bytes == 0) {
            return;
        }
        while (bytes > 0) {
            if (available() == 0) {
                if (!refresh() || available() == 0) {
                    throw new OrcCorruptionException(input.getOrcDataSourceId(), "Unexpected end of stream");
                }
            }
            long consume = Math.min(bytes, available());
            position += toIntExact(consume);
            bytes -= consume;
        }
    }

    public void skipVarints(long items)
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
        // If items to skip is > SIZE_OF_LONG it is safe to skip entire longs
        while (items - skipped > SIZE_OF_LONG && available() > MAX_VARINT_LENGTH) {
            long value = ByteArrays.getLong(buffer, position);
            position += SIZE_OF_LONG;
            long mask = ~value & VARINT_MASK;
            skipped += Long.bitCount(mask);
        }
        while (skipped < items && available() > 0) {
            if ((buffer[position++] & 0x80) == 0) {
                skipped++;
            }
        }
        return skipped;
    }

    public void seekToCheckpoint(long checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint);
        sync(input.peek());
    }
}
