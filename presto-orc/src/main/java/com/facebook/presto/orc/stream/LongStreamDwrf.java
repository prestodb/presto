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

import com.facebook.presto.orc.Vector;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.primitives.Ints;

import java.io.IOException;

import static com.facebook.presto.orc.stream.LongDecode.readDwrfLong;
import static com.google.common.base.Preconditions.checkPositionIndex;

public class LongStreamDwrf
        implements LongStream
{
    private final OrcInputStream input;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;

    public LongStreamDwrf(OrcInputStream input, OrcTypeKind type, boolean signed, boolean usesVInt)
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
        LongStreamDwrfCheckpoint dwrfCheckpoint = OrcStreamUtils.checkType(checkpoint, LongStreamDwrfCheckpoint.class, "Checkpoint");
        input.seekToCheckpoint(dwrfCheckpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        // there is no fast way to skip values
        for (int i = 0; i < items; i++) {
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
        checkPositionIndex(items, vector.length);
        checkPositionIndex(items, Vector.MAX_VECTOR_LENGTH);

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
}
