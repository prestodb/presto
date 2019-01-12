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
package io.prestosql.orc.stream;

import com.google.common.collect.ImmutableList;
import io.prestosql.orc.OrcOutputBuffer;
import io.prestosql.orc.checkpoint.FloatStreamCheckpoint;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.Stream;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static java.lang.Math.toIntExact;

public class FloatOutputStream
        implements ValueOutputStream<FloatStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FloatOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;
    private final List<FloatStreamCheckpoint> checkpoints = new ArrayList<>();

    private boolean closed;

    public FloatOutputStream(CompressionKind compression, int bufferSize)
    {
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    public void writeFloat(float value)
    {
        checkState(!closed);
        buffer.writeFloat(value);
    }

    @Override
    public void close()
    {
        closed = true;
        buffer.close();
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new FloatStreamCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public List<FloatStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public StreamDataOutput getStreamDataOutput(int column)
    {
        return new StreamDataOutput(buffer::writeDataTo, new Stream(column, DATA, toIntExact(buffer.getOutputDataSize()), false));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.estimateOutputDataSize();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize();
    }

    @Override
    public void reset()
    {
        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}
