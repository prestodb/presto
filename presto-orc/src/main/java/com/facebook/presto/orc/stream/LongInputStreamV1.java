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
import com.facebook.presto.orc.checkpoint.LongStreamV1Checkpoint;
import com.facebook.presto.orc.stream.aria.BufferConsumer;
import io.airlift.slice.ByteArrays;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.zigzagDecode;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class LongInputStreamV1
        implements LongInputStream
{
    private static final int MIN_REPEAT_SIZE = 3;
    private static final int MAX_LITERAL_SIZE = 128;
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final int VARINT_MASK32 = 0x8080_8080;
    private static final int BYTES_IN_LONG = SIZE_OF_LONG / SIZE_OF_BYTE;
    private static final int BYTES_IN_INT = SIZE_OF_INT / SIZE_OF_BYTE;

    private final OrcInputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;
    private PositionsFilter positionsFilter;

    public LongInputStreamV1(OrcInputStream input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
        lastReadInputCheckpoint = input.getCheckpoint();
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

    public void setPositionsFilter(int[] positions, boolean useLegacy)
    {
        if (useLegacy) {
            positionsFilter = new LegacyPositionsFilter(positions);
        }
        else {
            positionsFilter = new SkippingPositionsFilter(positions);
        }
    }

    public long nextLong()
            throws IOException
    {
        return positionsFilter.nextLong();
    }

    interface PositionsFilter
    {
        long nextLong()
                throws IOException;
    }

    private class LegacyPositionsFilter
            implements PositionsFilter
    {
        private int[] positions;
        private int offset;
        private int currentPosition;

        public LegacyPositionsFilter(int[] positions)
        {
            this.positions = positions;
        }
        public long nextLong()
                throws IOException
        {
            while (currentPosition < positions[offset]) {
                next();
                currentPosition++;
            }
            long result = next();
            offset++;
            currentPosition++;
            return result;
        }
    }

    private class SkippingPositionsFilter
            implements PositionsFilter
    {
        private int[] positions;
        private int offset;

        private int currentStartPosition;
        private int currentEndPosition;
        private LongBufferConsumer longBufferConsumer = new LongBufferConsumer();
        private int currentLiteralsToProcess;

        public SkippingPositionsFilter(int[] positions)
        {
            this.positions = positions;
        }

        public long nextLong()
                throws IOException
        {
            while (positions[offset] >= currentEndPosition) {
                if (!repeat) {
                    longBufferConsumer.skip(currentLiteralsToProcess);
                }

                used = 0;
                int control = longBufferConsumer.read();
                if (control == -1) {
                    throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
                }
                currentStartPosition = currentEndPosition;
                if (control < 0x80) {
                    numLiterals = control + MIN_REPEAT_SIZE;
                    repeat = true;
                    delta = longBufferConsumer.read();
                    if (delta == -1) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
                    }
                    delta = (byte) delta;
                    literals[0] = longBufferConsumer.decodeVint();
                }
                else {
                    numLiterals = 0x100 - control;
                    repeat = false;
                    delta = 0;
                    currentLiteralsToProcess = numLiterals;
                }
                currentEndPosition += numLiterals;
            }
            long result;
            if (repeat) {
                used = positions[offset] - currentStartPosition;
                result = literals[0] + used * delta;
            }
            else {
                int processed = numLiterals - currentLiteralsToProcess;
                int skipCount = positions[offset] - currentStartPosition - processed;
                longBufferConsumer.skip(skipCount);
                result = longBufferConsumer.decodeVint();
                currentLiteralsToProcess -= skipCount + 1;
            }
            offset++;
            return result;
        }
    }
    private class LongBufferConsumer
            extends BufferConsumer
    {
        @Override
        public void skip(int items)
                throws IOException
        {
            if (items == 0) {
                return;
            }
            checkState(items > 0, "items to skip < 0");

            int count = 0;
            long value = 0;
            while (items > 0 && remaining() >= SIZE_OF_LONG) {
                value = ByteArrays.getLong(buffer, position);
                count = BYTES_IN_LONG - Long.bitCount(value & VARINT_MASK);
                items -= count;
                position += SIZE_OF_LONG;
            }
            if (items == 0) {
                long inverted = (value & VARINT_MASK) ^ VARINT_MASK;
                position -= Long.numberOfLeadingZeros(inverted) >>> 3;
                return;
            }
            if (items < 0) {
                items += count;
                position -= SIZE_OF_LONG;
            }

            if (items > 0 && remaining() >= SIZE_OF_INT) {
                value = ByteArrays.getInt(buffer, position);
                count = BYTES_IN_INT - Integer.bitCount((int) value & VARINT_MASK32);
                items -= count;
                position += SIZE_OF_INT;
            }
            if (items == 0) {
                int inverted = ((int) value & VARINT_MASK32) ^ VARINT_MASK32;
                position -= Long.numberOfLeadingZeros(inverted) >>> 3;
                return;
            }
            if (items < 0) {
                items += count;
                position -= SIZE_OF_INT;
            }

            while (items > 0 && remaining() > 0) {
                if ((0x80 & this.read()) == 0) {
                    items--;
                }
            }
            if (items > 0) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "EOF while reading unsigned vint");
            }
        }

        public long decodeVint()
                throws IOException
        {
            long result = 0;
            long offset = 0;
            long b;
            do {
                b = read();
                if (b == -1) {
                    throw new OrcCorruptionException(input.getOrcDataSourceId(), "EOF while reading unsigned vint");
                }
                result |= (b & 0b0111_1111) << offset;
                offset += 7;
            }
            while ((b & 0b1000_0000) != 0);
            if (signed) {
                return zigzagDecode(result);
            }
            else {
                return result;
            }
        }
        protected void refresh()
                throws IOException
        {
            input.readBuffer(this);
        }
    }
}
