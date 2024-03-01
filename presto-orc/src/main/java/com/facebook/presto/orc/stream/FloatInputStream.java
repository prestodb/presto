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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.checkpoint.FloatStreamCheckpoint;

import java.io.IOException;

import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static java.lang.Float.floatToRawIntBits;

public class FloatInputStream
        implements ValueInputStream<FloatStreamCheckpoint>
{
    private final OrcInputStream input;

    public FloatInputStream(OrcInputStream input)
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
    public void skip(long items)
            throws IOException
    {
        long length = items * SIZE_OF_FLOAT;
        input.skipFully(length);
    }

    public float next()
            throws IOException
    {
        return input.readFloat();
    }

    public void nextVector(Type type, int items, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            type.writeLong(builder, floatToRawIntBits(next()));
        }
    }
}
