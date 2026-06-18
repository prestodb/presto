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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.UnsafeSlice.getIntUnchecked;
import static io.airlift.slice.UnsafeSlice.getLongUnchecked;
import static io.airlift.slice.UnsafeSlice.getShortUnchecked;

public final class LongBitPacker
{
    // ORC uses no more than 9 bits to store run lengths (https://orc.apache.org/docs/run-length.html#direct)
    private static final int MAX_BUFFERED_POSITIONS = 512;

    // We use this temp buffer to work around poor read performance of single bytes from Slice.
    // Benchmarks show that reading from this byte[] is ~3x faster, even after accounting for the
    // extra write to this buffer, than reading byte at a time from the InputStream.
    private final byte[] tmp = new byte[SIZE_OF_LONG * MAX_BUFFERED_POSITIONS];
    private final Slice slice = Slices.wrappedBuffer(tmp);

    // TODO: refactor calling code, so that input can be a byte[]. (See comment above about performance)
    public void unpack(long[] buffer, int offset, int len, int bitSize, InputStream input)
            throws IOException
    {
        checkArgument(len <= MAX_BUFFERED_POSITIONS, "Expected ORC files to have runs of at most 512 bit packed longs");
        switch (bitSize) {
            case 1:
                unpack1(buffer, offset, len, input);
                break;
            case 2:
                unpack2(buffer, offset, len, input);
                break;
            case 4:
                unpack4(buffer, offset, len, input);
                break;
            case 8:
                unpack8(buffer, offset, len, input);
                break;
            case 16:
                unpack16(buffer, offset, len, input);
                break;
            case 24:
                unpack24(buffer, offset, len, input);
                break;
            case 32:
                unpack32(buffer, offset, len, input);
                break;
            case 40:
                unpack40(buffer, offset, len, input);
                break;
            case 48:
                unpack48(buffer, offset, len, input);
                break;
            case 56:
                unpack56(buffer, offset, len, input);
                break;
            case 64:
                unpack64(buffer, offset, len, input);
                break;
            default:
                unpackGeneric(buffer, offset, len, bitSize, input);
        }
    }

    private static void unpackGeneric(long[] buffer, int offset, int len, int bitSize, InputStream input)
            throws IOException
    {
        int bitsLeft = 0;
        int current = 0;

        for (int i = offset; i < (offset + len); i++) {
            long result = 0;
            int bitsLeftToRead = bitSize;
            while (bitsLeftToRead > bitsLeft) {
                result <<= bitsLeft;
                result |= current & ((1 << bitsLeft) - 1);
                bitsLeftToRead -= bitsLeft;
                current = input.read();
                bitsLeft = 8;
            }

            // handle the left over bits
            if (bitsLeftToRead > 0) {
                result <<= bitsLeftToRead;
                bitsLeft -= bitsLeftToRead;
                result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
            }
            buffer[i] = result;
        }
    }

    private void unpack1(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        if (len != 0 && len < 8) {
            unpack1Unaligned(buffer, offset, len, input.read());
            return;
        }

        int blockReadableBytes = (len + 7) / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        int outputIndex = offset;
        int end = offset + len;
        int tmpIndex = 0;
        for (; outputIndex + 7 < end; outputIndex += 8) {
            long value;
            value = tmp[tmpIndex];
            tmpIndex++;
            buffer[outputIndex] = (0b1000_0000 & value) >>> 7;
            buffer[outputIndex + 1] = (0b0100_0000 & value) >>> 6;
            buffer[outputIndex + 2] = (0b0010_0000 & value) >>> 5;
            buffer[outputIndex + 3] = (0b0001_0000 & value) >>> 4;
            buffer[outputIndex + 4] = (0b0000_1000 & value) >>> 3;
            buffer[outputIndex + 5] = (0b0000_0100 & value) >>> 2;
            buffer[outputIndex + 6] = (0b0000_0010 & value) >>> 1;
            buffer[outputIndex + 7] = 0b0000_0001 & value;
        }

        // Get the last byte and decode it, if necessary
        if (outputIndex < end) {
            unpack1Unaligned(buffer, outputIndex, end - outputIndex, tmp[blockReadableBytes - 1]);
        }
    }

    private static void unpack1Unaligned(long[] buffer, int outputIndex, int length, int value)
    {
        switch (length) {
            case 7:
                buffer[outputIndex + 6] = (0b0000_0010 & value) >>> 1;
                //noinspection fallthrough
            case 6:
                buffer[outputIndex + 5] = (0b0000_0100 & value) >>> 2;
                //noinspection fallthrough
            case 5:
                buffer[outputIndex + 4] = (0b0000_1000 & value) >>> 3;
                //noinspection fallthrough
            case 4:
                buffer[outputIndex + 3] = (0b0001_0000 & value) >>> 4;
                //noinspection fallthrough
            case 3:
                buffer[outputIndex + 2] = (0b0010_0000 & value) >>> 5;
                //noinspection fallthrough
            case 2:
                buffer[outputIndex + 1] = (0b0100_0000 & value) >>> 6;
                //noinspection fallthrough
            case 1:
                buffer[outputIndex] = (0b1000_0000 & value) >>> 7;
        }
    }

    private void unpack2(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        if (len != 0 && len < 4) {
            unpack2Unaligned(buffer, offset, len, input.read());
            return;
        }

        int blockReadableBytes = (2 * len + 7) / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        int outputIndex = offset;
        int end = offset + len;
        int tmpIndex = 0;
        for (; outputIndex + 3 < end; outputIndex += 4) {
            long value;
            value = tmp[tmpIndex];
            tmpIndex++;
            buffer[outputIndex] = (0b1100_0000 & value) >>> 6;
            buffer[outputIndex + 1] = (0b0011_0000 & value) >>> 4;
            buffer[outputIndex + 2] = (0b0000_1100 & value) >>> 2;
            buffer[outputIndex + 3] = 0b0000_0011 & value;
        }

        // Get the last byte and decode it, if necessary
        if (outputIndex < end) {
            unpack2Unaligned(buffer, outputIndex, end - outputIndex, tmp[blockReadableBytes - 1]);
        }
    }

    private static void unpack2Unaligned(long[] buffer, int outputIndex, int length, int value)
    {
        switch (length) {
            case 3:
                buffer[outputIndex + 2] = (0b0000_1100 & value) >>> 2;
                //noinspection fallthrough
            case 2:
                buffer[outputIndex + 1] = (0b0011_0000 & value) >>> 4;
                //noinspection fallthrough
            case 1:
                buffer[outputIndex] = (0b1100_0000 & value) >>> 6;
        }
    }

    private void unpack4(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        if (len != 0 && len < 3) {
            int value = input.read();
            buffer[offset] = (0b1111_0000 & value) >>> 4;
            if (len == 2) {
                buffer[offset + 1] = 0b0000_1111 & value;
            }
            return;
        }

        int blockReadableBytes = (4 * len + 7) / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        int outputIndex = offset;
        int end = offset + len;
        int tmpIndex = 0;
        for (; outputIndex + 1 < end; outputIndex += 2) {
            long value;
            value = tmp[tmpIndex];
            tmpIndex++;
            buffer[outputIndex] = (0b1111_0000 & value) >>> 4;
            buffer[outputIndex + 1] = 0b0000_1111 & value;
        }

        // Get the last byte and decode it, if necessary
        if (outputIndex != end) {
            buffer[outputIndex] = (0b1111_0000 & tmp[blockReadableBytes - 1]) >>> 4;
        }
    }

    private void unpack8(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        for (int i = 0; i < len; ) {
            i += input.read(tmp, i, len - i);
        }
        for (int i = 0; i < len; i++) {
            buffer[offset + i] = 0xFFL & tmp[i];
        }
    }

    private void unpack16(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 16 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            buffer[offset + i] = 0xFFFFL & Short.reverseBytes(getShortUnchecked(slice, 2 * i));
        }
    }

    private void unpack24(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 24 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            // It's safe to read 4-bytes at a time and shift, because slice is a view over tmp,
            // which has 8 bytes of buffer space for every position
            buffer[offset + i] = 0xFF_FFFFL & (Integer.reverseBytes(getIntUnchecked(slice, 3 * i)) >>> 8);
        }
    }

    private void unpack32(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 32 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            buffer[offset + i] = 0xFFFF_FFFFL & Integer.reverseBytes(getIntUnchecked(slice, 4 * i));
        }
    }

    private void unpack40(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 40 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            // It's safe to read 8-bytes at a time and shift, because slice is a view over tmp,
            // which has 8 bytes of buffer space for every position
            buffer[offset + i] = Long.reverseBytes(getLongUnchecked(slice, 5 * i)) >>> 24;
        }
    }

    private void unpack48(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 48 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            // It's safe to read 8-bytes at a time and shift, because slice is a view over tmp,
            // which has 8 bytes of buffer space for every position
            buffer[offset + i] = Long.reverseBytes(getLongUnchecked(slice, 6 * i)) >>> 16;
        }
    }

    private void unpack56(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 56 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            // It's safe to read 8-bytes at a time and shift, because slice is a view over tmp,
            // which has 8 bytes of buffer space for every position
            buffer[offset + i] = Long.reverseBytes(getLongUnchecked(slice, 7 * i)) >>> 8;
        }
    }

    private void unpack64(long[] buffer, int offset, int len, InputStream input)
            throws IOException
    {
        int blockReadableBytes = len * 64 / 8;
        for (int i = 0; i < blockReadableBytes; ) {
            i += input.read(tmp, i, blockReadableBytes - i);
        }
        for (int i = 0; i < len; i++) {
            buffer[offset + i] = Long.reverseBytes(getLongUnchecked(slice, 8 * i));
        }
    }
}
