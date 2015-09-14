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

import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.checkpoint.FloatStreamCheckpoint;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static com.facebook.presto.orc.stream.OrcStreamUtils.readFully;
import static com.facebook.presto.orc.stream.OrcStreamUtils.skipFully;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;

public class FloatStream
        implements ValueStream<FloatStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[OrcReader.MAX_BATCH_SIZE * SIZE_OF_FLOAT];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public FloatStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<FloatStreamCheckpoint> getCheckpointType()
    {
        return FloatStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(FloatStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        long length = items * SIZE_OF_FLOAT;
        skipFully(input, length);
    }

    public float next()
            throws IOException
    {
        readFully(input, buffer, 0, SIZE_OF_FLOAT);
        return slice.getFloat(0);
    }

    public void nextVector(int items, double[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);
        checkPositionIndex(items, OrcReader.MAX_BATCH_SIZE);

        // buffer that number of values
        readFully(input, buffer, 0, items * SIZE_OF_FLOAT);

        // load them into the buffer one at a time since we are reading
        // floats into a double vector
        int elementIndex = 0;
        for (int i = 0; i < items; i++) {
            vector[i] = slice.getFloat(elementIndex);
            elementIndex += SIZE_OF_FLOAT;
        }
    }

    public void nextVector(long items, double[] vector, boolean[] isNull)
            throws IOException
    {
        // count the number of non nulls
        int notNullCount = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                notNullCount++;
            }
        }

        // buffer that umber of values
        readFully(input, buffer, 0, notNullCount * SIZE_OF_FLOAT);

        // load them into the buffer
        int elementIndex = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = slice.getFloat(elementIndex);
                elementIndex += SIZE_OF_FLOAT;
            }
        }
    }
}
