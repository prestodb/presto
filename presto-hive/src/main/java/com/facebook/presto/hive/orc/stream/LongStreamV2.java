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
package com.facebook.presto.hive.orc.stream;

import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.ql.ErrorMsg;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;

/**
 * @see {@link org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerWriterV2} for description of various lightweight compression techniques.
 */
public class LongStreamV2
        implements LongStream
{
    private static final int MAX_LITERAL_SIZE = 512;

    private enum EncodingType
    {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
    }

    private final InputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int used;
    private final boolean skipCorrupt;

    public LongStreamV2(InputStream input, boolean signed, boolean skipCorrupt)
            throws IOException
    {
        this.input = input;
        this.signed = signed;
        this.skipCorrupt = skipCorrupt;
    }

    private void readValues()
            throws IOException
    {
        // read the first 2 bits and determine the encoding type
        int firstByte = input.read();
        if (firstByte < 0) {
            throw new EOFException("Read past end of RLE integer from " + input);
        }

        int enc = (firstByte >>> 6) & 0x03;
        if (EncodingType.SHORT_REPEAT.ordinal() == enc) {
            readShortRepeatValues(firstByte);
        }
        else if (EncodingType.DIRECT.ordinal() == enc) {
            readDirectValues(firstByte);
        }
        else if (EncodingType.PATCHED_BASE.ordinal() == enc) {
            readPatchedBaseValues(firstByte);
        }
        else {
            readDeltaValues(firstByte);
        }
    }

    private void readDeltaValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fb = (firstByte >>> 1) & 0x1f;
        if (fb != 0) {
            fb = LongDecode.decodeBitWidth(fb);
        }

        // extract the blob run length
        int len = (firstByte & 0x01) << 8;
        len |= input.read();

        // read the first value stored as vint
        long firstVal;
        firstVal = LongDecode.readVInt(signed, input);

        // store first value to result buffer
        long prevVal;
        literals[numLiterals++] = firstVal;

        // if fixed bits is 0 then all values have fixed delta
        if (fb == 0) {
            // read the fixed delta value stored as vint (deltas can be negative even
            // if all number are positive)
            long fd = LongDecode.readSignedVInt(input);

            // add fixed deltas to adjacent values
            for (int i = 0; i < len; i++) {
                literals[numLiterals++] = literals[numLiterals - 2] + fd;
            }
        }
        else {
            long deltaBase = LongDecode.readSignedVInt(input);
            // add delta base and first value
            literals[numLiterals++] = firstVal + deltaBase;
            prevVal = literals[numLiterals - 1];
            len -= 1;

            // write the unpacked values, add it to previous value and store final
            // value to result buffer. if the delta base value is negative then it
            // is a decreasing sequence else an increasing sequence
            readBitPackedLongs(literals, numLiterals, len, fb, input);
            while (len > 0) {
                if (deltaBase < 0) {
                    literals[numLiterals] = prevVal - literals[numLiterals];
                }
                else {
                    literals[numLiterals] = prevVal + literals[numLiterals];
                }
                prevVal = literals[numLiterals];
                len--;
                numLiterals++;
            }
        }
    }

    private void readPatchedBaseValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = LongDecode.decodeBitWidth(fbo);

        // extract the run length of data blob
        int len = (firstByte & 0x01) << 8;
        len |= input.read();
        // runs are always one off
        len += 1;

        // extract the number of bytes occupied by base
        int thirdByte = input.read();
        int bw = (thirdByte >>> 5) & 0x07;
        // base width is one off
        bw += 1;

        // extract patch width
        int pwo = thirdByte & 0x1f;
        int pw = LongDecode.decodeBitWidth(pwo);

        // read fourth byte and extract patch gap width
        int fourthByte = input.read();
        int pgw = (fourthByte >>> 5) & 0x07;
        // patch gap width is one off
        pgw += 1;

        // extract the length of the patch list
        int pl = fourthByte & 0x1f;

        // read the next base width number of bytes to extract base value
        long base = bytesToLongBE(input, bw);
        long mask = (1L << ((bw * 8) - 1));
        // if MSB of base value is 1 then base is negative value else positive
        if ((base & mask) != 0) {
            base = base & ~mask;
            base = -base;
        }

        // unpack the data blob
        long[] unpacked = new long[len];
        readBitPackedLongs(unpacked, 0, len, fb, input);

        // unpack the patch blob
        long[] unpackedPatch = new long[pl];

        if ((pw + pgw) > 64 && !skipCorrupt) {
            throw new IOException(ErrorMsg.ORC_CORRUPTED_READ.getMsg());
        }
        int bitSize = LongDecode.getClosestFixedBits(pw + pgw);
        readBitPackedLongs(unpackedPatch, 0, pl, bitSize, input);

        // apply the patch directly when decoding the packed data
        int patchIdx = 0;
        long currGap;
        long currPatch;
        long patchMask = ((1L << pw) - 1);
        currGap = unpackedPatch[patchIdx] >>> pw;
        currPatch = unpackedPatch[patchIdx] & patchMask;
        long actualGap = 0;

        // special case: gap is >255 then patch value will be 0.
        // if gap is <=255 then patch value cannot be 0
        while (currGap == 255 && currPatch == 0) {
            actualGap += 255;
            patchIdx++;
            currGap = unpackedPatch[patchIdx] >>> pw;
            currPatch = unpackedPatch[patchIdx] & patchMask;
        }
        // add the left over gap
        actualGap += currGap;

        // unpack data blob, patch it (if required), add base to get final result
        for (int i = 0; i < unpacked.length; i++) {
            if (i == actualGap) {
                // extract the patch value
                long patchedVal = unpacked[i] | (currPatch << fb);

                // add base to patched value
                literals[numLiterals++] = base + patchedVal;

                // increment the patch to point to next entry in patch list
                patchIdx++;

                if (patchIdx < pl) {
                    // read the next gap and patch
                    currGap = unpackedPatch[patchIdx] >>> pw;
                    currPatch = unpackedPatch[patchIdx] & patchMask;
                    actualGap = 0;

                    // special case: gap is >255 then patch will be 0. if gap is
                    // <=255 then patch cannot be 0
                    while (currGap == 255 && currPatch == 0) {
                        actualGap += 255;
                        patchIdx++;
                        currGap = unpackedPatch[patchIdx] >>> pw;
                        currPatch = unpackedPatch[patchIdx] & patchMask;
                    }
                    // add the left over gap
                    actualGap += currGap;

                    // next gap is relative to the current gap
                    actualGap += i;
                }
            }
            else {
                // no patching required. add base to unpacked value to get final value
                literals[numLiterals++] = base + unpacked[i];
            }
        }

    }

    private void readDirectValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fbo = (firstByte >>> 1) & 0x1f;
        int fb = LongDecode.decodeBitWidth(fbo);

        // extract the run length
        int len = (firstByte & 0x01) << 8;
        len |= input.read();
        // runs are one off
        len += 1;

        // write the unpacked values and zigzag decode to result buffer
        readBitPackedLongs(literals, numLiterals, len, fb, input);
        if (signed) {
            for (int i = 0; i < len; i++) {
                literals[numLiterals] = LongDecode.zigzagDecode(literals[numLiterals]);
                numLiterals++;
            }
        }
        else {
            numLiterals += len;
        }
    }

    private void readShortRepeatValues(int firstByte)
            throws IOException
    {
        // read the number of bytes occupied by the value
        int size = (firstByte >>> 3) & 0x07;
        // #bytes are one off
        size += 1;

        // read the run length
        int len = firstByte & 0x07;
        // run lengths values are stored only after MIN_REPEAT value is met
        len += MIN_REPEAT_SIZE;

        // read the repeated value which is store using fixed bytes
        long val = bytesToLongBE(input, size);

        if (signed) {
            val = LongDecode.zigzagDecode(val);
        }

        // repeat the value for length times
        for (int i = 0; i < len; i++) {
            literals[numLiterals++] = val;
        }
    }

    @Override
    public long next()
            throws IOException
    {
        if (used == numLiterals) {
            numLiterals = 0;
            used = 0;
            readValues();
        }
        return literals[used++];
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        while (items > 0) {
            if (used == numLiterals) {
                numLiterals = 0;
                used = 0;
                readValues();
            }
            long consume = Math.min(items, numLiterals - used);
            used += consume;
            items -= consume;
        }
    }

    @Override
    public long sum(int items)
            throws IOException
    {
        long sum = 0;
        for (int i = 0; i < items; i++) {
            sum += next();
        }
        return sum;
    }

    @Override
    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = next();
        }
    }

    @Override
    public void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = next();
            }
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = Ints.checkedCast(next());
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = Ints.checkedCast(next());
            }
        }
    }

    private static void readBitPackedLongs(long[] buffer, int offset, int len, int bitSize, InputStream input)
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

    /**
     * Read n bytes in big endian order and convert to long.
     */
    private static long bytesToLongBE(InputStream input, int n)
            throws IOException
    {
        long out = 0;
        long val;
        while (n > 0) {
            n--;
            // store it in a long and then shift else integer overflow will occur
            val = input.read();
            out |= (val << (n * 8));
        }
        return out;
    }
}
