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

import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.readDwrfLong;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static java.lang.Math.toIntExact;

public class LongInputStreamDwrf
        implements LongInputStream
{
    private final OrcInputStream input;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;

    public LongInputStreamDwrf(OrcInputStream input, OrcTypeKind type, boolean signed, boolean usesVInt)
    {
        this.input = input;
        this.orcTypeKind = type;
        this.signed = signed;
        this.usesVInt = usesVInt;
    }

    @Override
    public Class<LongStreamDwrfCheckpoint> getCheckpointType()
    {
        return LongStreamDwrfCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(LongStreamCheckpoint checkpoint)
            throws IOException
    {
        LongStreamDwrfCheckpoint dwrfCheckpoint = (LongStreamDwrfCheckpoint) checkpoint;
        input.seekToCheckpoint(dwrfCheckpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        // there is no fast way to skip values
        for (long i = 0; i < items; i++) {
            next();
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
    public long next()
            throws IOException
    {
        return readDwrfLong(input, orcTypeKind, signed, usesVInt);
    }

    @Override
    public void nextIntVector(int items, int[] vector)
            throws IOException
    {
        nextIntVector(items, vector, 0);
    }

    @Override
    public void nextIntVector(int items, int[] vector, int offset)
            throws IOException
    {
        checkPositionIndex(items + offset, vector.length);

        for (int i = offset; i < items + offset; i++) {
            vector[i] = toIntExact(next());
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException
    {
        nextIntVector(items, vector, 0, isNull);
    }

    @Override
    public void nextIntVector(int items, int[] vector, int vectorOffset, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i + vectorOffset] = toIntExact(next());
            }
        }
    }

    @Override
    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);

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
}
