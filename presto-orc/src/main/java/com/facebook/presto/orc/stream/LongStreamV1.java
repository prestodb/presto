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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;

import static com.facebook.presto.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;
import static java.lang.Math.toIntExact;

public class LongStreamV1
        implements LongStream
{
    private static final int MAX_LITERAL_SIZE = 128;

    private final OrcInputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;
    private long lastReadInputCheckpoint;

    public LongStreamV1(OrcInputStream input, boolean signed)
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
            throw new OrcCorruptionException("Read past end of RLE integer from %s", input);
        }

        if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new OrcCorruptionException("End of stream in RLE Integer from %s", input);
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
        LongStreamV1Checkpoint v1Checkpoint = OrcStreamUtils.checkType(checkpoint, LongStreamV1Checkpoint.class, "Checkpoint");

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
            vector[i] = toIntExact(next());
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = toIntExact(next());
            }
        }
    }
}
