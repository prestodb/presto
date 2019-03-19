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
import com.facebook.presto.orc.stream.OrcInputStreamAria.Buffer;
import io.airlift.slice.ByteArrays;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.zigzagDecode;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.toIntExact;

public class ScanningLongInputStreamV1
        implements LongInputStream
{
    private static final int MIN_REPEAT_SIZE = 3;
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;

    private final OrcInputStreamAria input;
    private final boolean signed;
    private long literal;
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;

    // Position of the first value of the run in literals from the checkpoint.
    private int currentRunOffset;

    private byte[] buffer;
    private int position;
    private int length;

    public ScanningLongInputStreamV1(OrcInputStreamAria input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
        lastReadInputCheckpoint = input.getCheckpoint();
        refresh();
    }

    private int available()
    {
        return length - position;
    }

    private boolean refresh()
    {
        if (!input.hasNext()) {
            return false;
        }
        Buffer bufferContainer = input.next();
        buffer = bufferContainer.getBuffer();
        position = bufferContainer.getPosition();
        length = bufferContainer.getLength();
        return true;
    }

    private int read()
    {
        // If input has compressionKind == NONE then first bufferView will be empty
        if (available() == 0) {
            if (!refresh()) {
                return -1;
            }
        }
        return buffer[position++] & 0xff;
    }

    private void readHeader()
            throws IOException
    {
        lastReadInputCheckpoint = input.getCheckpoint();
        int control = read();
        if (control == -1) {
            throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
        }

        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = read();
            if (delta == -1) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literal = decodeVarint();
        }
        else {
            numLiterals = 0x100 - control;
            used = 0;
            repeat = false;
        }
    }

    @Override
    // This comes from the Apache Hive ORC code
    public long next()
            throws IOException
    {
        long result;
        if (used == numLiterals) {
            readHeader();
        }
        if (repeat) {
            result = literal + (used++) * delta;
        }
        else {
            result = decodeVarint();
            used++;
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
        if (lastReadInputCheckpoint == v1Checkpoint.getInputStreamCheckpoint() && v1Checkpoint.getOffset() <= numLiterals && v1Checkpoint.getOffset() >= used) {
            skip(v1Checkpoint.getOffset() - used);
            currentRunOffset = -used;
        }
        else {
            // otherwise, discard the buffer and start over
            input.seekToCheckpoint(v1Checkpoint.getInputStreamCheckpoint());
            numLiterals = 0;
            used = 0;
            currentRunOffset = -v1Checkpoint.getOffset();
            refresh();
            skip(v1Checkpoint.getOffset());
        }
    }

    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (used == numLiterals) {
                readHeader();
            }
            long consume = Math.min(items, numLiterals - used);
            used += toIntExact(consume);
            items -= skipVarintsInBuffer(consume);
            if (items != 0) {
                // A skip of multiple runs can take place at seeking
                // to checkpoint. Keep track of the start of the run
                // in literals for use by next scan().
//                currentRunOffset += toIntExact(consume); //numLiterals;
                currentRunOffset += numLiterals;
            }
        }
    }

    private long skipVarintsInBuffer(long items)
            throws IOException
    {
        if (items == 0) {
            return 0;
        }
        if (repeat) {
            return items;
        }
        if (available() == 0) {
            if (!refresh() || available() == 0) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "Unexpected EOF");
            }
        }
        long skipped = 0;
        long mask = 0;
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
        }
        return skipped;
    }

    private long decodeVarint()
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

    public int scan(
            int[] offsets,
            int beginOffset,
            int numOffsets,
            int endOffset,
            ResultsConsumer resultsConsumer)
            throws IOException
    {
        int numResults = 0;
        for (int offsetIdx = beginOffset; offsetIdx < beginOffset + numOffsets; ) {
            if (used == numLiterals) {
                currentRunOffset += numLiterals;
                readHeader();
            }

            if (offsets[offsetIdx] - currentRunOffset >= numLiterals) {
                skip(numLiterals - used);
            }
            else {
                skip(offsets[offsetIdx] - currentRunOffset - used);
                if (resultsConsumer.consume(offsetIdx, getValue(offsets[offsetIdx]))) {
                    numResults++;
                }
                offsetIdx++;
            }
        }
        skip(endOffset - offsets[beginOffset + numOffsets - 1] - 1);
        return numResults;
    }

    private long getValue(int offset)
            throws IOException
    {
        long result = 0;
        if (repeat) {
            used = offset - currentRunOffset;
            result = literal + (offset - currentRunOffset) * delta;
        }
        else {
            used++;
            result = decodeVarint();
        }
        return result;
    }
}
