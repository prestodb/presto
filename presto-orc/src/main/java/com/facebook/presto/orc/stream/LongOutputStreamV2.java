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

import com.facebook.presto.orc.OrcOutputBuffer;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamV2Checkpoint;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.encodeBitWidth;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.findClosestNumBits;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.getClosestAlignedFixedBits;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.getClosestFixedBits;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.isSafeSubtract;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.percentileBits;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.writeVslong;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.writeVulong;
import static com.facebook.presto.orc.stream.LongOutputStreamV2.SerializationUtils.zigzagEncode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class LongOutputStreamV2
        implements LongOutputStream
{
    private enum EncodingType
    {
        SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA;

        private int getOpCode()
        {
            return ordinal() << 6;
        }
    }

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongOutputStreamV2.class).instanceSize();
    private static final int MAX_SCOPE = 512;
    private static final int MIN_REPEAT = 3;
    private static final int MAX_SHORT_REPEAT_LENGTH = 10;

    private final StreamKind streamKind;
    private final OrcOutputBuffer buffer;
    private final List<LongStreamCheckpoint> checkpoints = new ArrayList<>();

    private long prevDelta;
    private int fixedRunLength;
    private int variableRunLength;
    private final long[] literals = new long[MAX_SCOPE];
    private final boolean signed;
    private int numLiterals;
    private final long[] zigzagLiterals = new long[MAX_SCOPE];
    private final long[] baseReducedLiterals = new long[MAX_SCOPE];
    private final long[] adjDeltas = new long[MAX_SCOPE];
    private long fixedDelta;
    private int zzBits90p;
    private int zzBits100p;
    private int brBits95p;
    private int brBits100p;
    private int bitsDeltaMax;
    private int patchWidth;
    private int patchGapWidth;
    private int patchLength;
    private long[] gapVsPatchList;
    private long min;
    private boolean isFixedDelta = true;
    private final SerializationUtils utils = new SerializationUtils();

    private boolean closed;

    public LongOutputStreamV2(CompressionParameters compressionParameters, boolean signed, StreamKind streamKind)
    {
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.buffer = new OrcOutputBuffer(compressionParameters, Optional.empty());
        this.signed = signed;
    }

    @Override
    // This comes from the Apache Hive ORC code
    // todo most of this should be rewritten for readability and performance
    public void writeLong(long value)
    {
        checkState(!closed);
        if (numLiterals == 0) {
            initializeLiterals(value);
            return;
        }

        if (numLiterals == 1) {
            prevDelta = value - literals[0];
            literals[numLiterals++] = value;
            // if both values are same count as fixed run else variable run
            if (value == literals[0]) {
                fixedRunLength = 2;
                variableRunLength = 0;
            }
            else {
                fixedRunLength = 0;
                variableRunLength = 2;
            }
            return;
        }

        // is fixed delta run?
        if (prevDelta == 0 && value == literals[numLiterals - 1]) {
            literals[numLiterals++] = value;

            // if variable run is non-zero then we are seeing repeating
            // values at the end of variable run in which case keep
            // updating variable and fixed runs
            if (variableRunLength > 0) {
                fixedRunLength = 2;
            }
            fixedRunLength += 1;

            // if fixed run met the minimum condition and if variable
            // run is non-zero then flush the variable run and shift the
            // tail fixed runs to start of the buffer
            if (fixedRunLength >= MIN_REPEAT && variableRunLength > 0) {
                numLiterals -= MIN_REPEAT;
                variableRunLength -= MIN_REPEAT - 1;
                // copy the tail fixed runs
                long[] tailValues = new long[MIN_REPEAT];
                System.arraycopy(literals, numLiterals, tailValues, 0, MIN_REPEAT);

                // determine variable encoding and flush values
                writeValues(determineEncoding());

                // shift tail fixed runs to beginning of the buffer
                for (long tailValue : tailValues) {
                    literals[numLiterals++] = tailValue;
                }
            }

            // if fixed runs reached max repeat length then write values
            if (fixedRunLength == MAX_SCOPE) {
                writeValues(determineEncoding());
            }
            return;
        }

        // variable delta run
        //
        // if fixed run length is non-zero and if it satisfies the
        // short repeat conditions then write the values as short repeats
        // else use delta encoding
        if (fixedRunLength >= MIN_REPEAT) {
            if (fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
                writeValues(EncodingType.SHORT_REPEAT);
            }
            else {
                isFixedDelta = true;
                writeValues(EncodingType.DELTA);
            }
        }

        // if fixed run length is <MIN_REPEAT and current value is
        // different from previous then treat it as variable run
        if (fixedRunLength > 0 && fixedRunLength < MIN_REPEAT) {
            if (value != literals[numLiterals - 1]) {
                variableRunLength = fixedRunLength;
                fixedRunLength = 0;
            }
        }

        // after writing values re-initialize the variables
        if (numLiterals == 0) {
            initializeLiterals(value);
        }
        else {
            // keep updating variable run lengths
            prevDelta = value - literals[numLiterals - 1];
            literals[numLiterals++] = value;
            variableRunLength += 1;

            // if variable run length reach the max scope, write it
            if (variableRunLength == MAX_SCOPE) {
                writeValues(determineEncoding());
            }
        }
    }

    private void initializeLiterals(long val)
    {
        literals[numLiterals++] = val;
        fixedRunLength = 1;
        variableRunLength = 1;
    }

    private EncodingType determineEncoding()
    {
        // we need to compute zigzag values for DIRECT encoding if we decide to
        // break early for delta overflows or for shorter runs
        if (signed) {
            for (int i1 = 0; i1 < numLiterals; i1++) {
                zigzagLiterals[i1] = zigzagEncode(literals[i1]);
            }
        }
        else {
            System.arraycopy(literals, 0, zigzagLiterals, 0, numLiterals);
        }

        zzBits100p = percentileBits(zigzagLiterals, 0, numLiterals, 1.0);

        // not a big win for shorter runs to determine encoding
        if (numLiterals <= MIN_REPEAT) {
            return EncodingType.DIRECT;
        }

        // DELTA encoding check

        // for identifying monotonic sequences
        boolean isIncreasing = true;
        boolean isDecreasing = true;
        this.isFixedDelta = true;

        this.min = literals[0];
        long max = literals[0];
        final long initialDelta = literals[1] - literals[0];
        long currDelta = initialDelta;
        long deltaMax = initialDelta;
        this.adjDeltas[0] = initialDelta;

        for (int i = 1; i < numLiterals; i++) {
            final long l1 = literals[i];
            final long l0 = literals[i - 1];
            currDelta = l1 - l0;
            min = Math.min(min, l1);
            max = Math.max(max, l1);

            isIncreasing &= (l0 <= l1);
            isDecreasing &= (l0 >= l1);

            isFixedDelta &= (currDelta == initialDelta);
            if (i > 1) {
                adjDeltas[i - 1] = Math.abs(currDelta);
                deltaMax = Math.max(deltaMax, adjDeltas[i - 1]);
            }
        }

        // its faster to exit under delta overflow condition without checking for
        // PATCHED_BASE condition as encoding using DIRECT is faster and has less
        // overhead than PATCHED_BASE
        if (!isSafeSubtract(max, min)) {
            return EncodingType.DIRECT;
        }

        // invariant - subtracting any number from any other in the literals after
        // this point won't overflow

        // if initialDelta is 0 then we cannot delta encode as we cannot identify
        // the sign of deltas (increasing or decreasing)
        if (initialDelta != 0) {
            // if min is equal to max then the delta is 0, this condition happens for
            // fixed values run >10 which cannot be encoded with SHORT_REPEAT
            if (min == max) {
                throw new IllegalStateException("currDelta should be zero");
            }

            if (isFixedDelta) {
                fixedDelta = currDelta;
                return EncodingType.DELTA;
            }

            // stores the number of bits required for packing delta blob in
            // delta encoding
            bitsDeltaMax = findClosestNumBits(deltaMax);

            // monotonic condition
            if (isIncreasing || isDecreasing) {
                return EncodingType.DELTA;
            }
        }

        // PATCHED_BASE encoding check

        // percentile values are computed for the zigzag encoded values. if the
        // number of bit requirement between 90th and 100th percentile varies
        // beyond a threshold, then we need to patch the values. if the variation
        // is not significant then we can use direct encoding
        zzBits90p = percentileBits(zigzagLiterals, 0, numLiterals, 0.9);

        // if difference in bits between 95th percentile and 100th percentile
        // of zigzag encoded values is 0 or 1, then the patch length will be 0,
        // so just use direct
        if (zzBits100p - zzBits90p <= 1) {
            return EncodingType.DIRECT;
        }

        // patching is done only on base reduced values.
        // remove base from literals
        for (int i = 0; i < numLiterals; i++) {
            baseReducedLiterals[i] = literals[i] - min;
        }

        // 95th percentile width is used to determine max allowed value
        // after which patching will be done
        brBits95p = percentileBits(baseReducedLiterals, 0, numLiterals, 0.95);

        // 100th percentile is used to compute the max patch width
        brBits100p = percentileBits(baseReducedLiterals, 0, numLiterals, 1.0);

        // check again if patching will be effective using base reduced values.
        if (brBits100p == brBits95p) {
            return EncodingType.DIRECT;
        }

        return EncodingType.PATCHED_BASE;
    }

    private void writeValues(EncodingType encoding)
    {
        if (numLiterals == 0) {
            return;
        }

        switch (encoding) {
            case SHORT_REPEAT:
                writeShortRepeatValues();
                break;
            case DIRECT:
                writeDirectValues();
                break;
            case PATCHED_BASE:
                writePatchedBaseValues();
                break;
            default:
                writeDeltaValues();
                break;
        }
        clearEncoder();
    }

    private void writeShortRepeatValues()
    {
        // get the value that is repeating, compute the bits and bytes required
        long repeatVal;
        if (signed) {
            repeatVal = zigzagEncode(literals[0]);
        }
        else {
            repeatVal = literals[0];
        }

        // todo there are better ways to do this
        int numBitsRepeatVal = findClosestNumBits(repeatVal);
        int numBytesRepeatVal;
        if (numBitsRepeatVal % 8 == 0) {
            numBytesRepeatVal = numBitsRepeatVal >>> 3;
        }
        else {
            numBytesRepeatVal = (numBitsRepeatVal >>> 3) + 1;
        }

        // write encoding type in top 2 bits
        int header = EncodingType.SHORT_REPEAT.getOpCode();

        // write the number of bytes required for the value
        header |= ((numBytesRepeatVal - 1) << 3);

        // write the run length
        fixedRunLength -= MIN_REPEAT;
        header |= fixedRunLength;

        // write the header
        buffer.write(header);

        // write the repeating value in big endian byte order
        for (int i = numBytesRepeatVal - 1; i >= 0; i--) {
            int b = (int) ((repeatVal >>> (i * 8)) & 0xff);
            buffer.write(b);
        }

        fixedRunLength = 0;
    }

    private void writeDirectValues()
    {
        // write the number of fixed bits required in next 5 bits
        int fixedBits = getClosestAlignedFixedBits(zzBits100p);
        final int encodeBitWidth = encodeBitWidth(fixedBits) << 1;

        // adjust variable run length
        variableRunLength -= 1;

        // extract the 9th bit of run length
        final int tailBits = (variableRunLength & 0x100) >>> 8;

        // create first byte of the header
        final int headerFirstByte = EncodingType.DIRECT.getOpCode() | encodeBitWidth | tailBits;

        // second byte of the header stores the remaining 8 bits of run length
        final int headerSecondByte = variableRunLength & 0xff;

        // write header
        buffer.write(headerFirstByte);
        buffer.write(headerSecondByte);

        // bit packing the zigzag encoded literals
        utils.writeInts(zigzagLiterals, 0, numLiterals, fixedBits, buffer);

        // reset run length
        variableRunLength = 0;
    }

    private void writeDeltaValues()
    {
        int fixedBits = getClosestAlignedFixedBits(bitsDeltaMax);

        int length;
        int encodeBitWidth = 0;
        if (isFixedDelta) {
            // if fixed run length is greater than threshold then it will be fixed
            // delta sequence with delta value 0 else fixed delta sequence with
            // non-zero delta value
            if (fixedRunLength > MIN_REPEAT) {
                // ex. sequence: 2 2 2 2 2 2 2 2
                length = fixedRunLength - 1;
                fixedRunLength = 0;
            }
            else {
                // ex. sequence: 4 6 8 10 12 14 16
                length = variableRunLength - 1;
                variableRunLength = 0;
            }
        }
        else {
            // fixed width 0 is used for long repeating values.
            // sequences that require only 1 bit to encode will have an additional bit
            if (fixedBits == 1) {
                fixedBits = 2;
            }
            encodeBitWidth = encodeBitWidth(fixedBits) << 1;
            length = variableRunLength - 1;
            variableRunLength = 0;
        }

        // extract the 9th bit of run length
        final int tailBits = (length & 0x100) >>> 8;

        // create first byte of the header
        final int headerFirstByte = EncodingType.DELTA.getOpCode() | encodeBitWidth | tailBits;

        // second byte of the header stores the remaining 8 bits of runlength
        final int headerSecondByte = length & 0xff;

        // write header
        buffer.write(headerFirstByte);
        buffer.write(headerSecondByte);

        // store the first value from zigzag literal array
        if (signed) {
            writeVslong(buffer, literals[0]);
        }
        else {
            writeVulong(buffer, literals[0]);
        }

        if (isFixedDelta) {
            // if delta is fixed then we don't need to store delta blob
            writeVslong(buffer, fixedDelta);
        }
        else {
            // store the first value as delta value using zigzag encoding
            writeVslong(buffer, adjDeltas[0]);

            // adjacent delta values are bit packed. The length of adjDeltas array is
            // always one less than the number of literals (delta difference for n
            // elements is n-1). We have already written one element, write the
            // remaining numLiterals - 2 elements here
            utils.writeInts(adjDeltas, 1, numLiterals - 2, fixedBits, buffer);
        }
    }

    private void writePatchedBaseValues()
    {
        preparePatchedBlob();

        // NOTE: Aligned bit packing cannot be applied for PATCHED_BASE encoding
        // because patch is applied to MSB bits. For example: If fixed bit width of
        // base value is 7 bits and if patch is 3 bits, the actual value is
        // constructed by shifting the patch to left by 7 positions.
        // actual_value = patch << 7 | base_value
        // So, if we align base_value then actual_value can not be reconstructed.

        // write the number of fixed bits required in next 5 bits
        final int fb = brBits95p;
        final int efb = encodeBitWidth(fb) << 1;

        // adjust variable run length, they are one off
        variableRunLength -= 1;

        // extract the 9th bit of run length
        final int tailBits = (variableRunLength & 0x100) >>> 8;

        // create first byte of the header
        final int headerFirstByte = EncodingType.PATCHED_BASE.getOpCode() | efb | tailBits;

        // second byte of the header stores the remaining 8 bits of runlength
        final int headerSecondByte = variableRunLength & 0xff;

        // if the min value is negative toggle the sign
        final boolean isNegative = min < 0;
        if (isNegative) {
            min = -min;
        }

        // find the number of bytes required for base and shift it by 5 bits
        // to accommodate patch width. The additional bit is used to store the sign
        // of the base value.
        final int baseWidth = findClosestNumBits(min) + 1;
        final int baseBytes = baseWidth % 8 == 0 ? baseWidth / 8 : (baseWidth / 8) + 1;
        final int bb = (baseBytes - 1) << 5;

        // if the base value is negative then set MSB to 1
        if (isNegative) {
            min |= (1L << ((baseBytes * 8) - 1));
        }

        // third byte contains 3 bits for number of bytes occupied by base
        // and 5 bits for patchWidth
        final int headerThirdByte = bb | encodeBitWidth(patchWidth);

        // fourth byte contains 3 bits for page gap width and 5 bits for
        // patch length
        final int headerFourthByte = (patchGapWidth - 1) << 5 | patchLength;

        // write header
        buffer.write(headerFirstByte);
        buffer.write(headerSecondByte);
        buffer.write(headerThirdByte);
        buffer.write(headerFourthByte);

        // write the base value using fixed bytes in big endian order
        for (int i = baseBytes - 1; i >= 0; i--) {
            byte b = (byte) ((min >>> (i * 8)) & 0xff);
            buffer.write(b);
        }

        // base reduced literals are bit packed
        int closestFixedBits = getClosestFixedBits(fb);

        utils.writeInts(baseReducedLiterals, 0, numLiterals, closestFixedBits, buffer);

        // write patch list
        closestFixedBits = getClosestFixedBits(patchGapWidth + patchWidth);

        utils.writeInts(gapVsPatchList, 0, gapVsPatchList.length, closestFixedBits, buffer);

        // reset run length
        variableRunLength = 0;
    }

    private void preparePatchedBlob()
    {
        // mask will be max value beyond which patch will be generated
        long mask = (1L << brBits95p) - 1;

        // since we are considering only 95 percentile, the size of gap and
        // patch array can contain only be 5% values
        patchLength = (int) Math.ceil((numLiterals * 0.05));

        int[] gapList = new int[patchLength];
        long[] patchList = new long[patchLength];

        // #bit for patch
        patchWidth = brBits100p - brBits95p;
        patchWidth = getClosestFixedBits(patchWidth);

        // if patch bit requirement is 64 then it will not possible to pack
        // gap and patch together in a long. To make sure gap and patch can be
        // packed together adjust the patch width
        if (patchWidth == 64) {
            patchWidth = 56;
            brBits95p = 8;
            mask = (1L << brBits95p) - 1;
        }

        int gapIdx = 0;
        int patchIdx = 0;
        int prev = 0;
        int gap;
        int maxGap = 0;

        for (int i = 0; i < numLiterals; i++) {
            // if value is above mask then create the patch and record the gap
            if (baseReducedLiterals[i] > mask) {
                gap = i - prev;
                if (gap > maxGap) {
                    maxGap = gap;
                }

                // gaps are relative, so store the previous patched value index
                prev = i;
                gapList[gapIdx++] = gap;

                // extract the most significant bits that are over mask bits
                long patch = baseReducedLiterals[i] >>> brBits95p;
                patchList[patchIdx++] = patch;

                // strip off the MSB to enable safe bit packing
                baseReducedLiterals[i] &= mask;
            }
        }

        // adjust the patch length to number of entries in gap list
        patchLength = gapIdx;

        // if the element to be patched is the first and only element then
        // max gap will be 0, but to store the gap as 0 we need atleast 1 bit
        if (maxGap == 0 && patchLength != 0) {
            patchGapWidth = 1;
        }
        else {
            patchGapWidth = findClosestNumBits(maxGap);
        }

        // special case: if the patch gap width is greater than 256, then
        // we need 9 bits to encode the gap width. But we only have 3 bits in
        // header to record the gap width. To deal with this case, we will save
        // two entries in patch list in the following way
        // 256 gap width => 0 for patch value
        // actual gap - 256 => actual patch value
        // We will do the same for gap width = 511. If the element to be patched is
        // the last element in the scope then gap width will be 511. In this case we
        // will have 3 entries in the patch list in the following way
        // 255 gap width => 0 for patch value
        // 255 gap width => 0 for patch value
        // 1 gap width => actual patch value
        if (patchGapWidth > 8) {
            patchGapWidth = 8;
            // for gap = 511, we need two additional entries in patch list
            if (maxGap == 511) {
                patchLength += 2;
            }
            else {
                patchLength += 1;
            }
        }

        // create gap vs patch list
        gapIdx = 0;
        patchIdx = 0;
        gapVsPatchList = new long[patchLength];
        for (int i = 0; i < patchLength; i++) {
            long g = gapList[gapIdx++];
            long p = patchList[patchIdx++];
            while (g > 255) {
                gapVsPatchList[i++] = (255L << patchWidth);
                g -= 255;
            }

            // store patch value in LSBs and gap in MSBs
            gapVsPatchList[i] = (g << patchWidth) | p;
        }
    }

    private void clearEncoder()
    {
        numLiterals = 0;
        prevDelta = 0;
        fixedDelta = 0;
        zzBits90p = 0;
        zzBits100p = 0;
        brBits95p = 0;
        brBits100p = 0;
        bitsDeltaMax = 0;
        patchGapWidth = 0;
        patchLength = 0;
        patchWidth = 0;
        gapVsPatchList = null;
        min = 0;
        isFixedDelta = true;
    }

    public void flush()
    {
        if (numLiterals == 0) {
            return;
        }

        if (variableRunLength != 0) {
            writeValues(determineEncoding());
            return;
        }

        if (fixedRunLength == 0) {
            throw new IllegalStateException("literals does not agree with run length counters");
        }

        if (fixedRunLength < MIN_REPEAT) {
            variableRunLength = fixedRunLength;
            fixedRunLength = 0;
            writeValues(determineEncoding());
            return;
        }

        if (fixedRunLength >= MIN_REPEAT && fixedRunLength <= MAX_SHORT_REPEAT_LENGTH) {
            writeValues(EncodingType.SHORT_REPEAT);
            return;
        }

        isFixedDelta = true;
        writeValues(EncodingType.DELTA);
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new LongStreamV2Checkpoint(numLiterals, buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        flush();
        buffer.close();
    }

    @Override
    public List<LongStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public StreamDataOutput getStreamDataOutput(int column)
    {
        return new StreamDataOutput(buffer::writeDataTo, new Stream(column, streamKind, toIntExact(buffer.getOutputDataSize()), true));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.estimateOutputDataSize() + (Long.BYTES * numLiterals);
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE +
                buffer.getRetainedSize() +
                SizeOf.sizeOf(literals) +
                SizeOf.sizeOf(zigzagLiterals) +
                SizeOf.sizeOf(baseReducedLiterals) +
                SizeOf.sizeOf(adjDeltas) +
                SizeOf.sizeOf(gapVsPatchList);
    }

    @Override
    public void reset()
    {
        clearEncoder();

        closed = false;
        buffer.reset();
        checkpoints.clear();
    }

    // this entire class should be rewritten
    static final class SerializationUtils
    {
        private static final int BUFFER_SIZE = 64;
        private final byte[] writeBuffer = new byte[BUFFER_SIZE];

        static void writeVulong(SliceOutput output, long value)
        {
            while (true) {
                if ((value & ~0x7f) == 0) {
                    output.write((byte) value);
                    return;
                }
                else {
                    output.write((byte) (0x80 | (value & 0x7f)));
                    value >>>= 7;
                }
            }
        }

        static void writeVslong(SliceOutput output, long value)
        {
            writeVulong(output, (value << 1) ^ (value >> 63));
        }

        /**
         * Count the number of bits required to encode the given value
         */
        static int findClosestNumBits(long value)
        {
            int count = 0;
            while (value != 0) {
                count++;
                value = value >>> 1;
            }
            return getClosestFixedBits(count);
        }

        /**
         * zigzag encode the given value
         */
        static long zigzagEncode(long value)
        {
            return (value << 1) ^ (value >> 63);
        }

        /**
         * Compute the bits required to represent pth percentile value
         */
        static int percentileBits(long[] data, int offset, int length, double percentile)
        {
            checkArgument(percentile <= 1.0 && percentile > 0.0);

            // histogram that store the encoded bit requirement for each values.
            // maximum number of bits that can encoded is 32 (refer FixedBitSizes)
            int[] hist = new int[32];

            // compute the histogram
            for (int i = offset; i < (offset + length); i++) {
                int idx = encodeBitWidth(findClosestNumBits(data[i]));
                hist[idx] += 1;
            }

            int perLen = (int) (length * (1.0 - percentile));

            // return the bits required by pth percentile length
            for (int i = hist.length - 1; i >= 0; i--) {
                perLen -= hist[i];
                if (perLen < 0) {
                    return decodeBitWidth(i);
                }
            }

            return 0;
        }

        /**
         * For a given fixed bit this function will return the closest available fixed bit
         */
        static int getClosestFixedBits(int n)
        {
            if (n == 0) {
                return 1;
            }

            if (n >= 1 && n <= 24) {
                return n;
            }
            else if (n > 24 && n <= 26) {
                return 26;
            }
            else if (n > 26 && n <= 28) {
                return 28;
            }
            else if (n > 28 && n <= 30) {
                return 30;
            }
            else if (n > 30 && n <= 32) {
                return 32;
            }
            else if (n > 32 && n <= 40) {
                return 40;
            }
            else if (n > 40 && n <= 48) {
                return 48;
            }
            else if (n > 48 && n <= 56) {
                return 56;
            }
            else {
                return 64;
            }
        }

        public static int getClosestAlignedFixedBits(int n)
        {
            if (n == 0 || n == 1) {
                return 1;
            }
            else if (n > 1 && n <= 2) {
                return 2;
            }
            else if (n > 2 && n <= 4) {
                return 4;
            }
            else if (n > 4 && n <= 8) {
                return 8;
            }
            else if (n > 8 && n <= 16) {
                return 16;
            }
            else if (n > 16 && n <= 24) {
                return 24;
            }
            else if (n > 24 && n <= 32) {
                return 32;
            }
            else if (n > 32 && n <= 40) {
                return 40;
            }
            else if (n > 40 && n <= 48) {
                return 48;
            }
            else if (n > 48 && n <= 56) {
                return 56;
            }
            else {
                return 64;
            }
        }

        enum FixedBitSizes
        {
            ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
            THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
            TWENTY, TWENTY_ONE, TWENTY_TWO, TWENTY_THREE, TWENTY_FOUR, TWENTY_SIX,
            TWENTY_EIGHT, THIRTY, THIRTY_TWO, FORTY, FORTY_EIGHT, FIFTY_SIX, SIXTY_FOUR
        }

        /**
         * Finds the closest available fixed bit width match and returns its encoded
         * value (ordinal)
         *
         * @param n - fixed bit width to encode
         * @return encoded fixed bit width
         */
        static int encodeBitWidth(int n)
        {
            n = getClosestFixedBits(n);

            if (n >= 1 && n <= 24) {
                return n - 1;
            }
            else if (n > 24 && n <= 26) {
                return FixedBitSizes.TWENTY_SIX.ordinal();
            }
            else if (n > 26 && n <= 28) {
                return FixedBitSizes.TWENTY_EIGHT.ordinal();
            }
            else if (n > 28 && n <= 30) {
                return FixedBitSizes.THIRTY.ordinal();
            }
            else if (n > 30 && n <= 32) {
                return FixedBitSizes.THIRTY_TWO.ordinal();
            }
            else if (n > 32 && n <= 40) {
                return FixedBitSizes.FORTY.ordinal();
            }
            else if (n > 40 && n <= 48) {
                return FixedBitSizes.FORTY_EIGHT.ordinal();
            }
            else if (n > 48 && n <= 56) {
                return FixedBitSizes.FIFTY_SIX.ordinal();
            }
            else {
                return FixedBitSizes.SIXTY_FOUR.ordinal();
            }
        }

        /**
         * Decodes the ordinal fixed bit value to actual fixed bit width value
         */
        static int decodeBitWidth(int n)
        {
            if (n >= FixedBitSizes.ONE.ordinal() && n <= FixedBitSizes.TWENTY_FOUR.ordinal()) {
                return n + 1;
            }
            else if (n == FixedBitSizes.TWENTY_SIX.ordinal()) {
                return 26;
            }
            else if (n == FixedBitSizes.TWENTY_EIGHT.ordinal()) {
                return 28;
            }
            else if (n == FixedBitSizes.THIRTY.ordinal()) {
                return 30;
            }
            else if (n == FixedBitSizes.THIRTY_TWO.ordinal()) {
                return 32;
            }
            else if (n == FixedBitSizes.FORTY.ordinal()) {
                return 40;
            }
            else if (n == FixedBitSizes.FORTY_EIGHT.ordinal()) {
                return 48;
            }
            else if (n == FixedBitSizes.FIFTY_SIX.ordinal()) {
                return 56;
            }
            else {
                return 64;
            }
        }

        void writeInts(long[] input, int offset, int length, int bitSize, SliceOutput output)
        {
            requireNonNull(input, "input is null");
            checkArgument(input.length != 0);
            checkArgument(offset >= 0);
            checkArgument(length >= 1);
            checkArgument(bitSize >= 1);

            switch (bitSize) {
                case 1:
                    unrolledBitPack1(input, offset, length, output);
                    return;
                case 2:
                    unrolledBitPack2(input, offset, length, output);
                    return;
                case 4:
                    unrolledBitPack4(input, offset, length, output);
                    return;
                case 8:
                    unrolledBitPack8(input, offset, length, output);
                    return;
                case 16:
                    unrolledBitPack16(input, offset, length, output);
                    return;
                case 24:
                    unrolledBitPack24(input, offset, length, output);
                    return;
                case 32:
                    unrolledBitPack32(input, offset, length, output);
                    return;
                case 40:
                    unrolledBitPack40(input, offset, length, output);
                    return;
                case 48:
                    unrolledBitPack48(input, offset, length, output);
                    return;
                case 56:
                    unrolledBitPack56(input, offset, length, output);
                    return;
                case 64:
                    unrolledBitPack64(input, offset, length, output);
                    return;
            }

            // this is used by the patch base code
            int bitsLeft = 8;
            byte current = 0;
            for (int i = offset; i < (offset + length); i++) {
                long value = input[i];
                int bitsToWrite = bitSize;
                while (bitsToWrite > bitsLeft) {
                    // add the bits to the bottom of the current word
                    current |= value >>> (bitsToWrite - bitsLeft);
                    // subtract out the bits we just added
                    bitsToWrite -= bitsLeft;
                    // zero out the bits above bitsToWrite
                    value &= (1L << bitsToWrite) - 1;
                    output.write(current);
                    current = 0;
                    bitsLeft = 8;
                }
                bitsLeft -= bitsToWrite;
                current |= value << bitsLeft;
                if (bitsLeft == 0) {
                    output.write(current);
                    current = 0;
                    bitsLeft = 8;
                }
            }

            // flush
            if (bitsLeft != 8) {
                output.write(current);
            }
        }

        private static void unrolledBitPack1(long[] input, int offset, int len, SliceOutput output)
        {
            final int numHops = 8;
            final int remainder = len % numHops;
            final int endOffset = offset + len;
            final int endUnroll = endOffset - remainder;
            int val = 0;
            for (int i = offset; i < endUnroll; i = i + numHops) {
                val = (int) (val | ((input[i] & 1) << 7)
                        | ((input[i + 1] & 1) << 6)
                        | ((input[i + 2] & 1) << 5)
                        | ((input[i + 3] & 1) << 4)
                        | ((input[i + 4] & 1) << 3)
                        | ((input[i + 5] & 1) << 2)
                        | ((input[i + 6] & 1) << 1)
                        | (input[i + 7]) & 1);
                output.write(val);
                val = 0;
            }

            if (remainder > 0) {
                int startShift = 7;
                for (int i = endUnroll; i < endOffset; i++) {
                    val = (int) (val | (input[i] & 1) << startShift);
                    startShift -= 1;
                }
                output.write(val);
            }
        }

        private static void unrolledBitPack2(long[] input, int offset, int len, SliceOutput output)
        {
            final int numHops = 4;
            final int remainder = len % numHops;
            final int endOffset = offset + len;
            final int endUnroll = endOffset - remainder;
            int val = 0;
            for (int i = offset; i < endUnroll; i = i + numHops) {
                val = (int) (val | ((input[i] & 3) << 6)
                        | ((input[i + 1] & 3) << 4)
                        | ((input[i + 2] & 3) << 2)
                        | (input[i + 3]) & 3);
                output.write(val);
                val = 0;
            }

            if (remainder > 0) {
                int startShift = 6;
                for (int i = endUnroll; i < endOffset; i++) {
                    val = (int) (val | (input[i] & 3) << startShift);
                    startShift -= 2;
                }
                output.write(val);
            }
        }

        private static void unrolledBitPack4(long[] input, int offset, int len, SliceOutput output)
        {
            final int numHops = 2;
            final int remainder = len % numHops;
            final int endOffset = offset + len;
            final int endUnroll = endOffset - remainder;
            int val = 0;
            for (int i = offset; i < endUnroll; i = i + numHops) {
                val = (int) (val | ((input[i] & 15) << 4) | (input[i + 1]) & 15);
                output.write(val);
                val = 0;
            }

            if (remainder > 0) {
                int startShift = 4;
                for (int i = endUnroll; i < endOffset; i++) {
                    val = (int) (val | (input[i] & 15) << startShift);
                    startShift -= 4;
                }
                output.write(val);
            }
        }

        private void unrolledBitPack8(long[] input, int offset, int len, SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 1);
        }

        private void unrolledBitPack16(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 2);
        }

        private void unrolledBitPack24(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 3);
        }

        private void unrolledBitPack32(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 4);
        }

        private void unrolledBitPack40(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 5);
        }

        private void unrolledBitPack48(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 6);
        }

        private void unrolledBitPack56(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 7);
        }

        private void unrolledBitPack64(long[] input, int offset, int len,
                SliceOutput output)
        {
            unrolledBitPackBytes(input, offset, len, output, 8);
        }

        private void unrolledBitPackBytes(long[] input, int offset, int len, SliceOutput output, int numBytes)
        {
            final int numHops = 8;
            final int remainder = len % numHops;
            final int endOffset = offset + len;
            final int endUnroll = endOffset - remainder;
            int i = offset;
            for (; i < endUnroll; i = i + numHops) {
                writeLongBE(output, input, i, numHops, numBytes);
            }

            if (remainder > 0) {
                writeRemainingLongs(output, i, input, remainder, numBytes);
            }
        }

        private void writeRemainingLongs(SliceOutput output, int offset, long[] input, int remainder, int numBytes)
        {
            final int numHops = remainder;

            int idx = 0;
            switch (numBytes) {
                case 1:
                    while (remainder > 0) {
                        writeBuffer[idx] = (byte) (input[offset + idx] & 255);
                        remainder--;
                        idx++;
                    }
                    break;
                case 2:
                    while (remainder > 0) {
                        writeLongBE2(input[offset + idx], idx * 2);
                        remainder--;
                        idx++;
                    }
                    break;
                case 3:
                    while (remainder > 0) {
                        writeLongBE3(input[offset + idx], idx * 3);
                        remainder--;
                        idx++;
                    }
                    break;
                case 4:
                    while (remainder > 0) {
                        writeLongBE4(input[offset + idx], idx * 4);
                        remainder--;
                        idx++;
                    }
                    break;
                case 5:
                    while (remainder > 0) {
                        writeLongBE5(input[offset + idx], idx * 5);
                        remainder--;
                        idx++;
                    }
                    break;
                case 6:
                    while (remainder > 0) {
                        writeLongBE6(input[offset + idx], idx * 6);
                        remainder--;
                        idx++;
                    }
                    break;
                case 7:
                    while (remainder > 0) {
                        writeLongBE7(input[offset + idx], idx * 7);
                        remainder--;
                        idx++;
                    }
                    break;
                case 8:
                    while (remainder > 0) {
                        writeLongBE8(input[offset + idx], idx * 8);
                        remainder--;
                        idx++;
                    }
                    break;
                default:
                    break;
            }

            final int toWrite = numHops * numBytes;
            output.write(writeBuffer, 0, toWrite);
        }

        private void writeLongBE(SliceOutput output, long[] input, int offset, int numHops, int numBytes)
        {
            switch (numBytes) {
                case 1:
                    writeBuffer[0] = (byte) (input[offset + 0] & 255);
                    writeBuffer[1] = (byte) (input[offset + 1] & 255);
                    writeBuffer[2] = (byte) (input[offset + 2] & 255);
                    writeBuffer[3] = (byte) (input[offset + 3] & 255);
                    writeBuffer[4] = (byte) (input[offset + 4] & 255);
                    writeBuffer[5] = (byte) (input[offset + 5] & 255);
                    writeBuffer[6] = (byte) (input[offset + 6] & 255);
                    writeBuffer[7] = (byte) (input[offset + 7] & 255);
                    break;
                case 2:
                    writeLongBE2(input[offset + 0], 0);
                    writeLongBE2(input[offset + 1], 2);
                    writeLongBE2(input[offset + 2], 4);
                    writeLongBE2(input[offset + 3], 6);
                    writeLongBE2(input[offset + 4], 8);
                    writeLongBE2(input[offset + 5], 10);
                    writeLongBE2(input[offset + 6], 12);
                    writeLongBE2(input[offset + 7], 14);
                    break;
                case 3:
                    writeLongBE3(input[offset + 0], 0);
                    writeLongBE3(input[offset + 1], 3);
                    writeLongBE3(input[offset + 2], 6);
                    writeLongBE3(input[offset + 3], 9);
                    writeLongBE3(input[offset + 4], 12);
                    writeLongBE3(input[offset + 5], 15);
                    writeLongBE3(input[offset + 6], 18);
                    writeLongBE3(input[offset + 7], 21);
                    break;
                case 4:
                    writeLongBE4(input[offset + 0], 0);
                    writeLongBE4(input[offset + 1], 4);
                    writeLongBE4(input[offset + 2], 8);
                    writeLongBE4(input[offset + 3], 12);
                    writeLongBE4(input[offset + 4], 16);
                    writeLongBE4(input[offset + 5], 20);
                    writeLongBE4(input[offset + 6], 24);
                    writeLongBE4(input[offset + 7], 28);
                    break;
                case 5:
                    writeLongBE5(input[offset + 0], 0);
                    writeLongBE5(input[offset + 1], 5);
                    writeLongBE5(input[offset + 2], 10);
                    writeLongBE5(input[offset + 3], 15);
                    writeLongBE5(input[offset + 4], 20);
                    writeLongBE5(input[offset + 5], 25);
                    writeLongBE5(input[offset + 6], 30);
                    writeLongBE5(input[offset + 7], 35);
                    break;
                case 6:
                    writeLongBE6(input[offset + 0], 0);
                    writeLongBE6(input[offset + 1], 6);
                    writeLongBE6(input[offset + 2], 12);
                    writeLongBE6(input[offset + 3], 18);
                    writeLongBE6(input[offset + 4], 24);
                    writeLongBE6(input[offset + 5], 30);
                    writeLongBE6(input[offset + 6], 36);
                    writeLongBE6(input[offset + 7], 42);
                    break;
                case 7:
                    writeLongBE7(input[offset + 0], 0);
                    writeLongBE7(input[offset + 1], 7);
                    writeLongBE7(input[offset + 2], 14);
                    writeLongBE7(input[offset + 3], 21);
                    writeLongBE7(input[offset + 4], 28);
                    writeLongBE7(input[offset + 5], 35);
                    writeLongBE7(input[offset + 6], 42);
                    writeLongBE7(input[offset + 7], 49);
                    break;
                case 8:
                    writeLongBE8(input[offset + 0], 0);
                    writeLongBE8(input[offset + 1], 8);
                    writeLongBE8(input[offset + 2], 16);
                    writeLongBE8(input[offset + 3], 24);
                    writeLongBE8(input[offset + 4], 32);
                    writeLongBE8(input[offset + 5], 40);
                    writeLongBE8(input[offset + 6], 48);
                    writeLongBE8(input[offset + 7], 56);
                    break;
                default:
                    break;
            }

            final int toWrite = numHops * numBytes;
            output.write(writeBuffer, 0, toWrite);
        }

        private void writeLongBE2(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 0);
        }

        private void writeLongBE3(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 0);
        }

        private void writeLongBE4(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 24);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 3] = (byte) (val >>> 0);
        }

        private void writeLongBE5(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 32);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 24);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 3] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 4] = (byte) (val >>> 0);
        }

        private void writeLongBE6(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 40);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 32);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 24);
            writeBuffer[wbOffset + 3] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 4] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 5] = (byte) (val >>> 0);
        }

        private void writeLongBE7(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 48);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 40);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 32);
            writeBuffer[wbOffset + 3] = (byte) (val >>> 24);
            writeBuffer[wbOffset + 4] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 5] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 6] = (byte) (val >>> 0);
        }

        private void writeLongBE8(long val, int wbOffset)
        {
            writeBuffer[wbOffset + 0] = (byte) (val >>> 56);
            writeBuffer[wbOffset + 1] = (byte) (val >>> 48);
            writeBuffer[wbOffset + 2] = (byte) (val >>> 40);
            writeBuffer[wbOffset + 3] = (byte) (val >>> 32);
            writeBuffer[wbOffset + 4] = (byte) (val >>> 24);
            writeBuffer[wbOffset + 5] = (byte) (val >>> 16);
            writeBuffer[wbOffset + 6] = (byte) (val >>> 8);
            writeBuffer[wbOffset + 7] = (byte) (val >>> 0);
        }

        // Do not want to use Guava LongMath.checkedSubtract() here as it will throw
        // ArithmeticException in case of overflow
        public static boolean isSafeSubtract(long left, long right)
        {
            return (left ^ right) >= 0 | (left ^ (left - right)) >= 0;
        }
    }
}
