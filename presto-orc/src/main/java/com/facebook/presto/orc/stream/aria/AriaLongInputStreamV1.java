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
package com.facebook.presto.orc.stream.aria;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamV1Checkpoint;
import com.facebook.presto.orc.stream.LongDecode;
import com.facebook.presto.orc.stream.LongInputStream;
import io.airlift.slice.ByteArrays;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class AriaLongInputStreamV1
        implements LongInputStream
{
    private static final int MIN_REPEAT_SIZE = 3;
    private static final int MAX_LITERAL_SIZE = 128;
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final int VARINT_MASK32 = 0x8080_8080;

    private final AriaOrcInputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;

    private Positions positions;
    private final Range range = new Range();

    private byte[] buffer;
    private int bufferPosition;
    private int bufferLength;

    public AriaLongInputStreamV1(AriaOrcInputStream input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    private void readValuesAria()
            throws IOException
    {
        readHeader();
        while (!range.contains(positions.get())) {
            if (!repeat) {
                refreshBuffer();
                skipVarintsFully(numLiterals);
            }
            readHeader();
        }
        if (!repeat) {
            refreshBuffer();
            int skipCount = positions.get() - range.getBegin();
            while (range.contains(positions.get())) {
                refreshBuffer();
                skipVarintsFully(skipCount);
                do {
                    literals[used++] = LongDecode.readVInt(signed, input);
                    positions.advance();
                }
                while (!positions.isEmpty() && positions.isConsecutive() && range.contains(positions.get()));

                if (positions.isEmpty()) {
                    break;
                }
                skipCount = positions.getSkipCount();
            }
            refreshBuffer();
            skipVarintsFully(range.getEnd() - positions.get() + skipCount);
            numLiterals = used;
            used = 0;
        }
    }

    private void readHeader()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
        }
        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literals[0] = LongDecode.readVInt(signed, input);
        }
        else {
            numLiterals = 0x100 - control;
            repeat = false;
        }
        used = 0;
        range.setNextRange(numLiterals);
    }

    /**
     * Force input to advance if buffer is used up.
     * Adjust this.bufferPosition to account for the extra read.
     */
    private void advanceAndRefreshBuffer()
            throws IOException
    {
        input.advance();
        refreshBuffer();
    }

    /**
     * Fetch the underlying buffer from the AriaOrcInputStream
     *
     * NOTE: if the input stream is advanced with input.read()
     * the buffer, bufferPosition or bufferLength may be modified
     */
    private void refreshBuffer()
    {
        buffer = input.getBufferNotSafe();
        bufferPosition = input.getBufferPositionNotSafe();
        bufferLength = bufferPosition + input.available();
    }

    /**
     * Syncs input position with this.bufferPosition
     */
    private void syncToInput()
            throws IOException
    {
        input.skip(Math.max(0, input.available() - bufferLength + bufferPosition));
    }

    private void skipVarintsFully(int items)
            throws IOException
    {
        for (int skipCount = items; skipCount > 0; ) {
            skipCount -= skipVarints(skipCount);
            if (skipCount > 0 && input.available() == 0) {
                advanceAndRefreshBuffer();
            }
        }
    }

    /**
     * Skips varints in the current buffer
     * @param items number of items to skip
     * @return number varints skipped
     */
    private int skipVarints(int items)
            throws IOException
    {
        int remaining = Math.min(numLiterals - used, items);
        checkState(remaining >= 0, "remaining < 0");
        if (remaining == 0) {
            return items - remaining;
        }

        if (bufferPosition + SIZE_OF_LONG <= bufferLength) {
            remaining = skipVarint8(remaining);
        }
        else if (bufferPosition + SIZE_OF_INT <= bufferLength) {
            remaining = skipVarint4(remaining);
        }
        else {
            remaining = skipVarint1(remaining);
        }
        syncToInput();
        return items - remaining;
    }

    /**
     * Skip varints 1 byte at a time
     * @param remaining remaining varints to skip
     * @return varints still remaining, i.e. if buffer is exhausted
     */
    private int skipVarint1(int remaining)
    {
        while (remaining > 0 && bufferPosition < bufferLength) {
            if ((buffer[bufferPosition++] & 0x80) == 0) {
                remaining--;
            }
        }
        return remaining;
    }

    /**
     * Skip varints 4 bytes at a time
     * @param remaining remaining varints to skip
     * @return varints still remaining, i.e. if buffer is exhausted
     */
    private int skipVarint4(int remaining)
    {
        int value = ByteArrays.getInt(buffer, bufferPosition);
        int mask = (value & VARINT_MASK32) ^ VARINT_MASK32;
        remaining -= Integer.bitCount(mask);
        bufferPosition += SIZE_OF_INT;
        if (remaining < 0) {
            for (; remaining < 0; remaining++) {
                mask ^= Integer.highestOneBit(mask);
            }
            bufferPosition -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else if (remaining == 0) {
            bufferPosition -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else {
            remaining = skipVarint1(remaining);
        }
        return remaining;
    }

    /**
     * Skip varints 8 bytes at a time
     * @param remaining remaining varints to skip
     * @return varints still remaining, i.e. if buffer is exhausted
     */
    private int skipVarint8(int remaining)
    {
        long mask = 0;
        while (remaining > 0 && bufferPosition + SIZE_OF_LONG <= bufferLength) {
            long value = ByteArrays.getLong(buffer, bufferPosition);
            mask = (value & VARINT_MASK) ^ VARINT_MASK;
            remaining -= Long.bitCount(mask);
            bufferPosition += SIZE_OF_LONG;
        }
        if (remaining < 0) {
            for (; remaining < 0; remaining++) {
                mask ^= Long.highestOneBit(mask);
            }
            bufferPosition -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else if (remaining == 0) {
            bufferPosition -= Long.numberOfLeadingZeros(mask) >> 3;
        }
        else {
            remaining = skipVarint1(remaining);
        }
        return remaining;
    }

    public long nextLong()
            throws IOException
    {
        long result;
        if (repeat && !range.contains(positions.get()) || (!repeat && used == numLiterals)) {
            readValuesAria();
        }
        if (repeat) {
            result = literals[0] + (range.getRelativePosition(positions.get())) * delta;
            positions.advance();
        }
        else {
            result = literals[used++];
        }
        return result;
    }

    // This comes from the Apache Hive ORC code
    private void readValues()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();

        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
        }

        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literals[0] = LongDecode.readVInt(signed, input);
        }
        else {
            numLiterals = 0x100 - control;
            used = 0;
            repeat = false;
            for (int i = 0; i < numLiterals; ++i) {
                literals[i] = LongDecode.readVInt(signed, input);
            }
        }
    }

    @Override
    // This comes from the Apache Hive ORC code
    public long next()
            throws IOException
    {
        long result;
        if (used == numLiterals) {
            readValues();
        }
        if (repeat) {
            result = literals[0] + (used++) * delta;
        }
        else {
            result = literals[used++];
        }
        return result;
    }

    @Override
    public Class<? extends LongStreamV1Checkpoint> getCheckpointType()
    {
        return LongStreamV1Checkpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamV1Checkpoint v1Checkpoint = (LongStreamV1Checkpoint) checkpoint;

        // if the checkpoint is within the current buffer, just adjust the pointer
        if (lastReadInputCheckpoint == v1Checkpoint.getInputStreamCheckpoint() && v1Checkpoint.getOffset() <= numLiterals) {
            used = v1Checkpoint.getOffset();
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(v1Checkpoint.getInputStreamCheckpoint());
            numLiterals = 0;
            used = 0;
            skip(v1Checkpoint.getOffset());
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (used == numLiterals) {
                readValues();
            }
            long consume = Math.min(items, numLiterals - used);
            used += consume;
            items -= consume;
        }
    }

    public void setPositions(int[] positions)
    {
        checkState(this.positions == null, "positions is already set");
        this.positions = new Positions(positions);
    }

    private static class Positions
    {
        private int[] positions;
        private int offset;

        public Positions(int[] positions)
        {
            this.positions = positions;
        }

        public int get()
        {
            return positions[offset];
        }

        public void advance()
        {
            offset++;
        }

        public boolean isConsecutive()
        {
            return offset > 0 && positions[offset] - positions[offset - 1] == 1;
        }

        public boolean isEmpty()
        {
            return offset >= positions.length;
        }

        public int getSkipCount()
        {
            return positions[offset] - positions[offset - 1] - 1;
        }
    }

    private static class Range
    {
        // Begin offset in current buffer
        private int begin;
        // End offset exclusive
        private int end;

        public void setNextRange(int end)
        {
            this.begin = this.end;
            this.end += end;
        }

        public boolean contains(int offset)
        {
            return offset < end;
        }

        public int getRelativePosition(int offset)
        {
            return offset - begin;
        }

        public int getBegin()
        {
            return begin;
        }

        public int getEnd()
        {
            return end;
        }
    }
}
