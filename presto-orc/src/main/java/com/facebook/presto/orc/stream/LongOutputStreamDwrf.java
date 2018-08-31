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

import com.facebook.presto.orc.ChainedSliceLoader;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcOutputBuffer;
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.checkpoint.LongStreamDwrfCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.ChunkedSliceInput;
import io.airlift.slice.FixedLengthSliceInput;
import org.openjdk.jol.info.ClassLayout;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.stream.LongDecode.writeVLong;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class LongOutputStreamDwrf
        implements LongOutputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongOutputStreamDwrf.class).instanceSize();
    private final StreamKind streamKind;
    private final CompressionKind compression;
    private final int bufferSize;
    private final OrcOutputBuffer buffer;
    private final boolean signed;
    private final List<LongStreamDwrfCheckpoint> checkpoints = new ArrayList<>();

    private boolean closed;

    public LongOutputStreamDwrf(CompressionKind compression, int bufferSize, boolean signed, StreamKind streamKind)
    {
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.bufferSize = bufferSize;
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
        this.signed = signed;
    }

    @Override
    public void writeLong(long value)
    {
        checkState(!closed);

        writeVLong(buffer, value, signed);
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new LongStreamDwrfCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        buffer.close();
    }

    @Override
    public List<LongStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public LongInputStream getLongInputStream()
    {
        FixedLengthSliceInput sliceInput = new ChunkedSliceInput(new ChainedSliceLoader(buffer.getCompressedSlices()), 32 * 1024);
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("LongOutputStream");
        try {
            return new LongInputStreamDwrf(
                    new OrcInputStream(
                            orcDataSourceId,
                            sliceInput,
                            createOrcDecompressor(orcDataSourceId, compression, bufferSize),
                            newSimpleAggregatedMemoryContext(),
                            sliceInput.getRetainedSize()),
                    OrcTypeKind.LONG,
                    signed,
                    true);
        }
        catch (OrcCorruptionException e) {
            throw new UncheckedIOException("Unable to create LongInputStream from LongOutputStream", e);
        }
    }

    @Override
    public StreamDataOutput getStreamDataOutput(int column)
    {
        return new StreamDataOutput(buffer::writeDataTo, new Stream(column, streamKind, toIntExact(buffer.getOutputDataSize()), true));
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
