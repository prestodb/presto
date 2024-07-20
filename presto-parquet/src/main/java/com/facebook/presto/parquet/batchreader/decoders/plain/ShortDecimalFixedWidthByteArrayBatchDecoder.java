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
package com.facebook.presto.parquet.batchreader.decoders.plain;

import com.facebook.presto.parquet.batchreader.SimpleSliceInputStream;

import static com.facebook.presto.parquet.batchreader.BytesUtils.propagateSignBit;
import static com.google.common.base.Preconditions.checkArgument;

public class ShortDecimalFixedWidthByteArrayBatchDecoder
{
    private static final ShortDecimalDecoder[] VALUE_DECODERS = new ShortDecimalDecoder[] {
            new BigEndianReader1(),
            new BigEndianReader2(),
            new BigEndianReader3(),
            new BigEndianReader4(),
            new BigEndianReader5(),
            new BigEndianReader6(),
            new BigEndianReader7(),
            new BigEndianReader8()
    };

    public interface ShortDecimalDecoder
    {
        void decode(SimpleSliceInputStream input, long[] values, int offset, int length);
    }

    private final ShortDecimalDecoder decoder;

    public ShortDecimalFixedWidthByteArrayBatchDecoder(int length)
    {
        checkArgument(
                length > 0 && length <= 8,
                "Short decimal length %s must be in range 1-8",
                length);
        decoder = VALUE_DECODERS[length - 1];
        // Unscaled number is encoded as two's complement using big-endian byte order
        // (the most significant byte is the zeroth element)
    }

    /**
     * This method uses Unsafe operations on Slice.
     * Always check if needed data is available with ensureBytesAvailable method.
     * Failing to do so may result in instant JVM crash.
     */
    public void getShortDecimalValues(SimpleSliceInputStream input, long[] values, int offset, int length)
    {
        decoder.decode(input, values, offset, length);
    }

    private static final class BigEndianReader8
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int endOffset = offset + length;
            for (int i = offset; i < endOffset; i++) {
                values[i] = Long.reverseBytes(input.readLongUnsafe());
            }
        }
    }

    private static final class BigEndianReader7
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 8;
                bytesOffSet += 7;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 7);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 6) & 0xFFL)
                    | (input.getByteUnsafe(index + 5) & 0xFFL) << 8
                    | (input.getByteUnsafe(index + 4) & 0xFFL) << 16
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 24;
            return propagateSignBit(value, 8);
        }
    }

    private static final class BigEndianReader6
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 16;
                bytesOffSet += 6;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 6);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 5) & 0xFFL)
                    | (input.getByteUnsafe(index + 4) & 0xFFL) << 8
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 16;
            return propagateSignBit(value, 16);
        }
    }

    private static final class BigEndianReader5
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = Long.reverseBytes(input.getLongUnsafe(bytesOffSet)) >> 24;
                bytesOffSet += 5;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, bytesOffSet);
            input.skip(bytesOffSet + 5);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 4) & 0xFFL)
                    | (Integer.reverseBytes(input.getIntUnsafe(index)) & 0xFFFFFFFFL) << 8;
            return propagateSignBit(value, 24);
        }
    }

    private static final class BigEndianReader4
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            while (length > 1) {
                long value = Long.reverseBytes(input.readLongUnsafe());

                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[offset] = (int) (value >> 32);
                values[offset + 1] = (int) value;

                offset += 2;
                length -= 2;
            }

            if (length > 0) {
                int value = input.readIntUnsafe();
                values[offset] = Integer.reverseBytes(value);
            }
        }
    }

    private static final class BigEndianReader3
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            int bytesOffSet = 0;
            int endOffset = offset + length;
            int i = offset;
            for (; i < endOffset - 2; i += 2) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                long value = Long.reverseBytes(input.getLongUnsafe(bytesOffSet));
                values[i] = value >> 40;
                values[i + 1] = value << 24 >> 40;
                bytesOffSet += 6;
            }
            // Decode the last values "normally" as it would read data out of bounds
            while (i < endOffset) {
                values[i++] = decode(input, bytesOffSet);
                bytesOffSet += 3;
            }
            input.skip(bytesOffSet);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnsafe(index + 2) & 0xFFL)
                    | (input.getByteUnsafe(index + 1) & 0xFFL) << 8
                    | (input.getByteUnsafe(index) & 0xFFL) << 16;
            return propagateSignBit(value, 40);
        }
    }

    private static final class BigEndianReader2
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            while (length > 3) {
                long value = input.readLongUnsafe();
                // Reverse all bytes at once
                value = Long.reverseBytes(value);

                // We first shift the byte as left as possible. Then, when shifting back right,
                // the sign bit will get propagated
                values[offset] = value >> 48;
                values[offset + 1] = (value << 16) >> 48;
                values[offset + 2] = (value << 32) >> 48;
                values[offset + 3] = (value << 48) >> 48;

                offset += 4;
                length -= 4;
            }

            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[offset++] = Short.reverseBytes(input.readShort());
                length--;
            }
        }
    }

    private static final class BigEndianReader1
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            while (length > 7) {
                long value = input.readLongUnsafe();

                // We first shift the byte as left as possible. Then, when shifting back right,
                // the sign bit will get propagated
                values[offset] = (value << 56) >> 56;
                values[offset + 1] = (value << 48) >> 56;
                values[offset + 2] = (value << 40) >> 56;
                values[offset + 3] = (value << 32) >> 56;
                values[offset + 4] = (value << 24) >> 56;
                values[offset + 5] = (value << 16) >> 56;
                values[offset + 6] = (value << 8) >> 56;
                values[offset + 7] = value >> 56;

                offset += 8;
                length -= 8;
            }

            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly
                values[offset++] = input.readByte();
                length--;
            }
        }
    }
}
