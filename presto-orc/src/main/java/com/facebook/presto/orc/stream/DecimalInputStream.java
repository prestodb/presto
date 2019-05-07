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
import com.facebook.presto.orc.checkpoint.DecimalStreamCheckpoint;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static com.google.common.base.Verify.verify;

public class DecimalInputStream
        implements ValueInputStream<DecimalStreamCheckpoint>
{
    private static final long SIGN_LONG_MASK = 1L << 63;
    private static final long LONG_MASK = 0x80_80_80_80_80_80_80_80L;
    private static final int INT_MASK = 0x80_80_80_80;

    private final OrcChunkLoader chunkLoader;

    private Slice block = Slices.EMPTY_SLICE; // reference to current (decoded) data block. Can be either a reference to the buffer or to current chunk
    private int blockOffset; // position within current data block
    private long lastCheckpoint;

    public DecimalInputStream(OrcChunkLoader chunkLoader)
    {
        this.chunkLoader = chunkLoader;
    }

    @Override
    public Class<? extends DecimalStreamCheckpoint> getCheckpointType()
    {
        return DecimalStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DecimalStreamCheckpoint checkpoint)
            throws IOException
    {
        long newCheckpoint = checkpoint.getInputStreamCheckpoint();
        // if checkpoint starts at the same compressed position...
        // (we're checking for an empty block because empty blocks signify that we possibly read all the data in the existing
        // buffer, so last checkpoint is no longer valid)
        if (block.length() > 0 && decodeCompressedBlockOffset(newCheckpoint) == decodeCompressedBlockOffset(lastCheckpoint)) {
            // and decompressed position is within our block, reposition in the block directly
            int blockOffset = decodeDecompressedOffset(newCheckpoint) - decodeDecompressedOffset(lastCheckpoint);
            if (blockOffset >= 0 && blockOffset < block.length()) {
                this.blockOffset = blockOffset;
                // do not change last checkpoint because we have not moved positions
                return;
            }
        }
        chunkLoader.seekToCheckpoint(newCheckpoint);
        lastCheckpoint = newCheckpoint;
        block = Slices.EMPTY_SLICE;
        blockOffset = 0;
    }

    // result must have at least batchSize * 2 capacity
    @SuppressWarnings("PointlessBitwiseExpression")
    public void nextLongDecimal(long[] result, int batchSize)
            throws IOException
    {
        verify(result.length >= batchSize * 2);

        int count = 0;

        while (count < batchSize) {
            if (blockOffset == block.length()) {
                advance();
            }

            while (blockOffset <= block.length() - 20) { // we'll read 2 longs + 1 int
                long low;
                long middle = 0;
                int high = 0;

                // low bits
                long current = block.getLong(blockOffset);
                int zeros = Long.numberOfTrailingZeros(~current & LONG_MASK);
                int end = (zeros + 1) / 8;
                blockOffset += end;

                boolean negative = (current & 1) == 1;

                low = (current & 0x7F_00_00_00_00_00_00_00L) >>> 7;
                low |= (current & 0x7F_00_00_00_00_00_00L) >>> 6;
                low |= (current & 0x7F_00_00_00_00_00L) >>> 5;
                low |= (current & 0x7F_00_00_00_00L) >>> 4;
                low |= (current & 0x7F_00_00_00) >>> 3;
                low |= (current & 0x7F_00_00) >>> 2;
                low |= (current & 0x7F_00) >>> 1;
                low |= (current & 0x7F) >>> 0;

                low = low & ((1L << (end * 7)) - 1);

                // middle bits
                if (zeros == 64) {
                    current = block.getLong(blockOffset);
                    zeros = Long.numberOfTrailingZeros(~current & LONG_MASK);
                    end = (zeros + 1) / 8;
                    blockOffset += end;

                    middle = (current & 0x7F_00_00_00_00_00_00_00L) >>> 7;
                    middle |= (current & 0x7F_00_00_00_00_00_00L) >>> 6;
                    middle |= (current & 0x7F_00_00_00_00_00L) >>> 5;
                    middle |= (current & 0x7F_00_00_00_00L) >>> 4;
                    middle |= (current & 0x7F_00_00_00) >>> 3;
                    middle |= (current & 0x7F_00_00) >>> 2;
                    middle |= (current & 0x7F_00) >>> 1;
                    middle |= (current & 0x7F) >>> 0;

                    middle = middle & ((1L << (end * 7)) - 1);

                    // high bits
                    if (zeros == 64) {
                        int last = block.getInt(blockOffset);
                        zeros = Integer.numberOfTrailingZeros(~last & INT_MASK);
                        end = (zeros + 1) / 8;

                        blockOffset += end;

                        high = (last & 0x7F_00_00) >>> 2;
                        high |= (last & 0x7F_00) >>> 1;
                        high |= (last & 0x7F) >>> 0;

                        high = high & ((1 << (end * 7)) - 1);

                        if (end == 4 || high > 0xFF_FF) { // only 127 - (55 + 56) = 16 bits allowed in high
                            throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal exceeds 128 bits");
                        }
                    }
                }

                emitLongDecimal(result, count, low, middle, high, negative);
                count++;

                if (count == batchSize) {
                    return;
                }
            }

            // handle the tail of the current block
            count = decodeLongDecimalTail(result, count, batchSize);
        }
    }

    private int decodeLongDecimalTail(long[] result, int count, int batchSize)
            throws IOException
    {
        boolean negative = false;
        long low = 0;
        long middle = 0;
        int high = 0;

        long value;
        boolean last = false;

        if (blockOffset == block.length()) {
            advance();
        }

        int offset = 0;
        while (true) {
            value = block.getByte(blockOffset);
            blockOffset++;

            if (offset == 0) {
                negative = (value & 1) == 1;
                low |= (value & 0x7F);
            }
            else if (offset < 8) {
                low |= (value & 0x7F) << (offset * 7);
            }
            else if (offset < 16) {
                middle |= (value & 0x7F) << ((offset - 8) * 7);
            }
            else if (offset < 19) {
                high |= (value & 0x7F) << ((offset - 16) * 7);
            }
            else {
                throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal exceeds 128 bits");
            }

            offset++;

            if ((value & 0x80) == 0) {
                if (high > 0xFF_FF) { // only 127 - (55 + 56) = 16 bits allowed in high
                    throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal exceeds 128 bits");
                }
                emitLongDecimal(result, count, low, middle, high, negative);
                count++;

                low = 0;
                middle = 0;
                high = 0;
                offset = 0;

                if (blockOffset == block.length()) {
                    // the last value aligns with the end of the block, so just
                    // reset the block and loop around to optimized decoding
                    break;
                }

                if (last || count == batchSize) {
                    break;
                }
            }
            else if (blockOffset == block.length()) {
                last = true;
                advance();
            }
        }
        return count;
    }

    private static void emitLongDecimal(long[] result, int offset, long low, long middle, long high, boolean negative)
    {
        long lower = (low >>> 1) | (middle << 55); // drop the sign bit from low
        long upper = (middle >>> 9) | (high << 47);

        if (negative) {
            if (lower == 0xFFFFFFFFFFFFFFFFL) {
                lower = 0;
                upper += 1;
            }
            else {
                lower += 1;
            }
        }

        result[2 * offset] = lower;
        result[2 * offset + 1] = upper | (negative ? SIGN_LONG_MASK : 0);
    }

    @SuppressWarnings("PointlessBitwiseExpression")
    public void nextShortDecimal(long[] result, int batchSize)
            throws IOException
    {
        verify(result.length >= batchSize);
        int count = 0;

        while (count < batchSize) {
            if (blockOffset == block.length()) {
                advance();
            }

            while (blockOffset <= block.length() - 12) { // we'll read 1 longs + 1 int
                long low;
                int high = 0;

                // low bits
                long current = block.getLong(blockOffset);
                int zeros = Long.numberOfTrailingZeros(~current & LONG_MASK);
                int end = (zeros + 1) / 8;
                blockOffset += end;

                low = (current & 0x7F_00_00_00_00_00_00_00L) >>> 7;
                low |= (current & 0x7F_00_00_00_00_00_00L) >>> 6;
                low |= (current & 0x7F_00_00_00_00_00L) >>> 5;
                low |= (current & 0x7F_00_00_00_00L) >>> 4;
                low |= (current & 0x7F_00_00_00) >>> 3;
                low |= (current & 0x7F_00_00) >>> 2;
                low |= (current & 0x7F_00) >>> 1;
                low |= (current & 0x7F) >>> 0;

                low = low & ((1L << (end * 7)) - 1);

                // high bits
                if (zeros == 64) {
                    int last = block.getInt(blockOffset);
                    zeros = Integer.numberOfTrailingZeros(~last & INT_MASK);
                    end = (zeros + 1) / 8;

                    blockOffset += end;

                    high = (last & 0x7F_00) >>> 1;
                    high |= (last & 0x7F) >>> 0;

                    high = high & ((1 << (end * 7)) - 1);

                    if (end >= 3 || high > 0xFF) { // only 63 - (55) = 8 bits allowed in high
                        throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal does not fit long (invalid table schema?)");
                    }
                }

                emitShortDecimal(result, count, low, high);
                count++;

                if (count == batchSize) {
                    return;
                }
            }

            // handle the tail of the current block
            count = decodeShortDecimalTail(result, count, batchSize);
        }
    }

    private int decodeShortDecimalTail(long[] result, int count, int batchSize)
            throws IOException
    {
        long low = 0;
        long high = 0;

        long value;
        boolean last = false;

        int offset = 0;

        if (blockOffset == block.length()) {
            advance();
        }

        while (true) {
            value = block.getByte(blockOffset);
            blockOffset++;

            if (offset == 0) {
                low |= (value & 0x7F);
            }
            else if (offset < 8) {
                low |= (value & 0x7F) << (offset * 7);
            }
            else if (offset < 11) {
                high |= (value & 0x7F) << ((offset - 8) * 7);
            }
            else {
                throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal does not fit long (invalid table schema?)");
            }

            offset++;

            if ((value & 0x80) == 0) {
                if (high > 0xFF) { // only 63 - (55) = 8 bits allowed in high
                    throw new OrcCorruptionException(chunkLoader.getOrcDataSourceId(), "Decimal does not fit long (invalid table schema?)");
                }

                emitShortDecimal(result, count, low, high);
                count++;

                low = 0;
                high = 0;
                offset = 0;

                if (blockOffset == block.length()) {
                    // the last value aligns with the end of the block, so just
                    // reset the block and loop around to optimized decoding
                    break;
                }

                if (last || count == batchSize) {
                    break;
                }
            }
            else if (blockOffset == block.length()) {
                last = true;
                advance();
            }
        }
        return count;
    }

    private static void emitShortDecimal(long[] result, int offset, long low, long high)
    {
        boolean negative = (low & 1) == 1;
        long value = (low >>> 1) | (high << 55); // drop the sign bit from low

        if (negative) {
            value += 1;
            value = -value;
        }

        result[offset] = value;
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        if (items == 0) {
            return;
        }

        if (blockOffset == block.length()) {
            advance();
        }

        int count = 0;
        while (true) {
            while (blockOffset <= block.length() - Long.BYTES) { // only safe if there's at least one long to read
                long current = block.getLong(blockOffset);

                int increment = Long.bitCount(~current & LONG_MASK);
                if (count + increment >= items) {
                    // reached the tail, so bail out and process byte at a time
                    break;
                }

                count += increment;
                blockOffset += Long.BYTES;
            }

            while (blockOffset < block.length()) { // tail -- byte at a time
                byte current = block.getByte(blockOffset);
                blockOffset++;

                if ((current & 0x80) == 0) {
                    count++;

                    if (count == items) {
                        return;
                    }
                }
            }

            advance();
        }
    }

    private void advance()
            throws IOException
    {
        block = chunkLoader.nextChunk();
        lastCheckpoint = chunkLoader.getLastCheckpoint();
        blockOffset = 0;
    }
}
