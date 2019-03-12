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

import java.io.IOException;

import static java.lang.Math.toIntExact;

public class LongInputStreamV1
        implements LongInputStream
{
    private static final int MIN_REPEAT_SIZE = 3;

    private final BufferConsumer bufferConsumer;
    private long literal;
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;

    // Position of the first value of the run in literals from the checkpoint.
    private int currentRunOffset;

    public LongInputStreamV1(OrcInputStream input, boolean signed)
    {
        this.bufferConsumer = new BufferConsumer(input, signed);
        lastReadInputCheckpoint = input.getCheckpoint();
    }

    private void readHeader()
            throws IOException
    {
        lastReadInputCheckpoint = bufferConsumer.getCheckpoint();
        int control = bufferConsumer.read();
        if (control == -1) {
            throw new OrcCorruptionException(bufferConsumer.getOrcDataSourceId(), "Read past end of RLE integer");
        }

        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = bufferConsumer.read();
            if (delta == -1) {
                throw new OrcCorruptionException(bufferConsumer.getOrcDataSourceId(), "End of stream in RLE Integer");
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literal = bufferConsumer.decodeVarint();
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
            result = bufferConsumer.decodeVarint();
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
            bufferConsumer.seekToCheckpoint(v1Checkpoint.getInputStreamCheckpoint());
            numLiterals = 0;
            used = 0;
            currentRunOffset = -v1Checkpoint.getOffset();
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
            if (repeat) {
                items -= consume;
            }
            else {
                items -= consume; //bufferConsumer.skipVarintsInBuffer(consume);
                bufferConsumer.skipVarints(consume);
            }
            if (items != 0) {
                // A skip of multiple runs can take place at seeking
                // to checkpoint. Keep track of the start of the run
                // in literals for use by next scan().
                currentRunOffset += numLiterals;
            }
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
                int endIndex = Math.min(offsetIdx + numLiterals - used, offsetIdx + beginOffset);
                do {
                    if (resultsConsumer.consume(offsetIdx, getValue(offsets[offsetIdx]))) {
                        numResults++;
                    }
                    offsetIdx++;
                }
                while (offsetIdx < endIndex && offsets[offsetIdx] - 1 == offsets[offsetIdx - 1]);
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
            result = bufferConsumer.decodeVarint();
        }
        return result;
    }
}
