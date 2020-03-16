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

import static java.lang.Math.min;

public class LongInputStreamV1
        implements LongInputStream
{
    private static final int MIN_REPEAT_SIZE = 3;

    private final OrcInputStream input;
    private final boolean signed;
    private long repeatBase;
    private int numValuesInRun;
    private int delta;
    private int used;
    private boolean repeat;

    public LongInputStreamV1(OrcInputStream input, boolean signed)
    {
        this.input = input;
        this.signed = signed;
    }

    private void readHeader()
            throws IOException
    {
        int control = input.read();
        if (control == -1) {
            throw new OrcCorruptionException(input.getOrcDataSourceId(), "Read past end of RLE integer");
        }

        if (control < 0x80) {
            numValuesInRun = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new OrcCorruptionException(input.getOrcDataSourceId(), "End of stream in RLE Integer");
            }

            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            delta = (byte) delta;
            repeatBase = input.readVarint(signed);
        }
        else {
            numValuesInRun = 0x100 - control;
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
        if (used == numValuesInRun) {
            readHeader();
        }
        if (repeat) {
            result = repeatBase + used * delta;
        }
        else {
            result = input.readVarint(signed);
        }
        used++;
        return result;
    }

    @Override
    public void next(long[] values, int items)
            throws IOException
    {
        int offset = 0;
        while (items > 0) {
            if (used == numValuesInRun) {
                numValuesInRun = 0;
                used = 0;
                readHeader();
            }

            int chunkSize = min(numValuesInRun - used, items);
            if (repeat) {
                for (int i = 0; i < chunkSize; i++) {
                    values[offset + i] = repeatBase + ((used + i) * delta);
                }
            }
            else {
                for (int i = 0; i < chunkSize; ++i) {
                    values[offset + i] = input.readVarint(signed);
                }
            }
            used += chunkSize;
            offset += chunkSize;
            items -= chunkSize;
        }
    }

    @Override
    public void next(int[] values, int items)
            throws IOException
    {
        int offset = 0;
        while (items > 0) {
            if (used == numValuesInRun) {
                numValuesInRun = 0;
                used = 0;
                readHeader();
            }

            int chunkSize = min(numValuesInRun - used, items);
            if (repeat) {
                for (int i = 0; i < chunkSize; i++) {
                    long literal = repeatBase + ((used + i) * delta);
                    int value = (int) literal;
                    if (literal != value) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "Decoded value out of range for a 32bit number");
                    }
                    values[offset + i] = value;
                }
            }
            else {
                for (int i = 0; i < chunkSize; i++) {
                    long literal = input.readVarint(signed);
                    int value = (int) literal;
                    if (literal != value) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "Decoded value out of range for a 32bit number");
                    }
                    values[offset + i] = value;
                }
            }
            used += chunkSize;
            offset += chunkSize;
            items -= chunkSize;
        }
    }

    @Override
    public void next(short[] values, int items)
            throws IOException
    {
        int offset = 0;
        while (items > 0) {
            if (used == numValuesInRun) {
                numValuesInRun = 0;
                used = 0;
                readHeader();
            }

            int chunkSize = min(numValuesInRun - used, items);
            if (repeat) {
                for (int i = 0; i < chunkSize; i++) {
                    long literal = repeatBase + ((used + i) * delta);
                    short value = (short) literal;
                    if (literal != value) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "Decoded value out of range for a 16bit number");
                    }
                    values[offset + i] = value;
                }
            }
            else {
                for (int i = 0; i < chunkSize; i++) {
                    long literal = input.readVarint(signed);
                    short value = (short) literal;
                    if (literal != value) {
                        throw new OrcCorruptionException(input.getOrcDataSourceId(), "Decoded value out of range for a 16bit number");
                    }
                    values[offset + i] = value;
                }
            }
            used += chunkSize;
            offset += chunkSize;
            items -= chunkSize;
        }
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
        // Discard the buffer and start over
        input.seekToCheckpoint(v1Checkpoint.getInputStreamCheckpoint());
        numValuesInRun = 0;
        used = 0;
        skip(v1Checkpoint.getOffset());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items > 0) {
            if (used == numValuesInRun) {
                readHeader();
            }
            long consume = Math.min(items, numValuesInRun - used);
            if (!repeat) {
                input.skipVarints(consume);
            }
            used += consume;
            items -= consume;
        }
    }
}
