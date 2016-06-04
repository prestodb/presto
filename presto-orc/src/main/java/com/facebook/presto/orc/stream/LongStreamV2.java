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
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamV2Checkpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;

/**
 * @see {@link org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerWriterV2} for description of various lightweight compression techniques.
 */
// This comes from the Apache Hive ORC code
public class LongStreamV2
        implements LongStream
{
    private static final int MAX_LITERAL_SIZE = 512;

    private enum EncodingType
    {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA
    }

    private final LongBitPacker packer = new LongBitPacker();
    private final OrcInputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int used;
    private final boolean skipCorrupt;
    private long lastReadInputCheckpoint;

    public LongStreamV2(OrcInputStream input, boolean signed, boolean skipCorrupt)
    {
        this.input = input;
        this.signed = signed;
        this.skipCorrupt = skipCorrupt;
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    // This comes from the Apache Hive ORC code
    private void readValues()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        // read the first 2 bits and determine the encoding type
        int firstByte = input.read();
        if (firstByte < 0) {
            throw new OrcCorruptionException("Read past end of RLE integer from %s", input);
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

    // This comes from the Apache Hive ORC code
    private void readDeltaValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fixedBits = (firstByte >>> 1) & 0x1f;
        if (fixedBits != 0) {
            fixedBits = LongDecode.decodeBitWidth(fixedBits);
        }

        // extract the blob run length
        int length = (firstByte & 0x01) << 8;
        length |= input.read();

        // read the first value stored as vint
        long firstVal = LongDecode.readVInt(signed, input);

        // store first value to result buffer
        literals[numLiterals++] = firstVal;

        // if fixed bits is 0 then all values have fixed delta
        long prevVal;
        if (fixedBits == 0) {
            // read the fixed delta value stored as vint (deltas can be negative even
            // if all number are positive)
            long fixedDelta = LongDecode.readSignedVInt(input);

            // add fixed deltas to adjacent values
            for (int i = 0; i < length; i++) {
                literals[numLiterals++] = literals[numLiterals - 2] + fixedDelta;
            }
        }
        else {
            long deltaBase = LongDecode.readSignedVInt(input);
            // add delta base and first value
            literals[numLiterals++] = firstVal + deltaBase;
            prevVal = literals[numLiterals - 1];
            length -= 1;

            // write the unpacked values, add it to previous value and store final
            // value to result buffer. if the delta base value is negative then it
            // is a decreasing sequence else an increasing sequence
            packer.unpack(literals, numLiterals, length, fixedBits, input);
            while (length > 0) {
                if (deltaBase < 0) {
                    literals[numLiterals] = prevVal - literals[numLiterals];
                }
                else {
                    literals[numLiterals] = prevVal + literals[numLiterals];
                }
                prevVal = literals[numLiterals];
                length--;
                numLiterals++;
            }
        }
    }

    // This comes from the Apache Hive ORC code
    private void readPatchedBaseValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fb = LongDecode.decodeBitWidth((firstByte >>> 1) & 0b1_1111);

        // extract the run length of data blob
        int length = (firstByte & 0b1) << 8;
        length |= input.read();
        // runs are always one off
        length += 1;

        // extract the number of bytes occupied by base
        int thirdByte = input.read();
        int baseWidth = (thirdByte >>> 5) & 0b0111;
        // base width is one off
        baseWidth += 1;

        // extract patch width
        int patchWidth = LongDecode.decodeBitWidth(thirdByte & 0b1_1111);

        // read fourth byte and extract patch gap width
        int fourthByte = input.read();
        int patchGapWidth = (fourthByte >>> 5) & 0b0111;
        // patch gap width is one off
        patchGapWidth += 1;

        // extract the length of the patch list
        int patchListLength = fourthByte & 0b1_1111;

        // read the next base width number of bytes to extract base value
        long base = bytesToLongBE(input, baseWidth);
        long mask = (1L << ((baseWidth * 8) - 1));
        // if MSB of base value is 1 then base is negative value else positive
        if ((base & mask) != 0) {
            base = base & ~mask;
            base = -base;
        }

        // unpack the data blob
        long[] unpacked = new long[length];
        packer.unpack(unpacked, 0, length, fb, input);

        // unpack the patch blob
        long[] unpackedPatch = new long[patchListLength];

        if ((patchWidth + patchGapWidth) > 64 && !skipCorrupt) {
            throw new OrcCorruptionException("ORC file is corrupt");
        }

        int bitSize = LongDecode.getClosestFixedBits(patchWidth + patchGapWidth);
        packer.unpack(unpackedPatch, 0, patchListLength, bitSize, input);

        // apply the patch directly when decoding the packed data
        int patchIndex = 0;
        long currentGap;
        long currentPatch;
        long patchMask = ((1L << patchWidth) - 1);
        currentGap = unpackedPatch[patchIndex] >>> patchWidth;
        currentPatch = unpackedPatch[patchIndex] & patchMask;
        long actualGap = 0;

        // special case: gap is >255 then patch value will be 0.
        // if gap is <=255 then patch value cannot be 0
        while (currentGap == 255 && currentPatch == 0) {
            actualGap += 255;
            patchIndex++;
            currentGap = unpackedPatch[patchIndex] >>> patchWidth;
            currentPatch = unpackedPatch[patchIndex] & patchMask;
        }
        // add the left over gap
        actualGap += currentGap;

        // unpack data blob, patch it (if required), add base to get final result
        for (int i = 0; i < unpacked.length; i++) {
            if (i == actualGap) {
                // extract the patch value
                long patchedValue = unpacked[i] | (currentPatch << fb);

                // add base to patched value
                literals[numLiterals++] = base + patchedValue;

                // increment the patch to point to next entry in patch list
                patchIndex++;

                if (patchIndex < patchListLength) {
                    // read the next gap and patch
                    currentGap = unpackedPatch[patchIndex] >>> patchWidth;
                    currentPatch = unpackedPatch[patchIndex] & patchMask;
                    actualGap = 0;

                    // special case: gap is >255 then patch will be 0. if gap is
                    // <=255 then patch cannot be 0
                    while (currentGap == 255 && currentPatch == 0) {
                        actualGap += 255;
                        patchIndex++;
                        currentGap = unpackedPatch[patchIndex] >>> patchWidth;
                        currentPatch = unpackedPatch[patchIndex] & patchMask;
                    }
                    // add the left over gap
                    actualGap += currentGap;

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

    // This comes from the Apache Hive ORC code
    private void readDirectValues(int firstByte)
            throws IOException
    {
        // extract the number of fixed bits
        int fixedBits = LongDecode.decodeBitWidth((firstByte >>> 1) & 0b1_1111);

        // extract the run length
        int length = (firstByte & 0b1) << 8;
        length |= input.read();
        // runs are one off
        length += 1;

        // write the unpacked values and zigzag decode to result buffer
        packer.unpack(literals, numLiterals, length, fixedBits, input);
        if (signed) {
            for (int i = 0; i < length; i++) {
                literals[numLiterals] = LongDecode.zigzagDecode(literals[numLiterals]);
                numLiterals++;
            }
        }
        else {
            numLiterals += length;
        }
    }

    // This comes from the Apache Hive ORC code
    private void readShortRepeatValues(int firstByte)
            throws IOException
    {
        // read the number of bytes occupied by the value
        int size = (firstByte >>> 3) & 0b0111;
        // #bytes are one off
        size += 1;

        // read the run length
        int length = firstByte & 0x07;
        // run lengths values are stored only after MIN_REPEAT value is met
        length += MIN_REPEAT_SIZE;

        // read the repeated value which is store using fixed bytes
        long val = bytesToLongBE(input, size);

        if (signed) {
            val = LongDecode.zigzagDecode(val);
        }

        // repeat the value for length times
        for (int i = 0; i < length; i++) {
            literals[numLiterals++] = val;
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
    public Class<LongStreamV2Checkpoint> getCheckpointType()
    {
        return LongStreamV2Checkpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamV2Checkpoint v2Checkpoint = OrcStreamUtils.checkType(checkpoint, LongStreamV2Checkpoint.class, "Checkpoint");

        // if the checkpoint is within the current buffer, just adjust the pointer
        if (lastReadInputCheckpoint == v2Checkpoint.getInputStreamCheckpoint() && v2Checkpoint.getOffset() <= numLiterals) {
            used = v2Checkpoint.getOffset();
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(v2Checkpoint.getInputStreamCheckpoint());
            numLiterals = 0;
            used = 0;
            skip(v2Checkpoint.getOffset());
        }
    }

    @Override
    public void skip(long items)
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
    public void nextLongVector(Type type, int items, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            type.writeLong(builder, next());
        }
    }

    @Override
    public void nextLongVector(Type type, int items, BlockBuilder builder, boolean[] isNull)
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
}
