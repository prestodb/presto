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

import com.facebook.presto.orc.checkpoint.DoubleStreamCheckpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static com.facebook.presto.orc.stream.OrcStreamUtils.readFully;
import static com.facebook.presto.orc.stream.OrcStreamUtils.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class DoubleInputStream
        implements ValueInputStream<DoubleStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[SIZE_OF_DOUBLE];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public DoubleInputStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<DoubleStreamCheckpoint> getCheckpointType()
    {
       return DoubleStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DoubleStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        long length = items * SIZE_OF_DOUBLE;
        skipFully(input, length);
    }

    public double next()
            throws IOException
    {
        readFully(input, buffer, 0, SIZE_OF_DOUBLE);
        return slice.getDouble(0);
    }

    public void nextVector(Type type, int items, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            type.writeDouble(builder, next());
        }
    }

    public void nextVector(Type type, long items, BlockBuilder builder, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (isNull[i]) {
                builder.appendNull();
            }
            else {
                type.writeDouble(builder, next());
            }
        }
    }
}
