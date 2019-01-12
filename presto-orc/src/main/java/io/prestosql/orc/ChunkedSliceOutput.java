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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Math.min;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;

public final class ChunkedSliceOutput
        extends SliceOutput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ChunkedSliceOutput.class).instanceSize();
    private static final int MINIMUM_CHUNK_SIZE = 4096;
    private static final int MAXIMUM_CHUNK_SIZE = 16 * 1024 * 1024;
    // This must not be larger than MINIMUM_CHUNK_SIZE/2
    private static final int MAX_UNUSED_BUFFER_SIZE = 128;

    private final ChunkSupplier chunkSupplier;

    private Slice slice;
    private byte[] buffer;

    private final List<Slice> closedSlices = new ArrayList<>();
    private long closedSlicesRetainedSize;

    /**
     * Offset of buffer within stream.
     */
    private long streamOffset;

    /**
     * Current position for writing in buffer.
     */
    private int bufferPosition;

    public ChunkedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        this.chunkSupplier = new ChunkSupplier(minChunkSize, maxChunkSize);

        this.buffer = chunkSupplier.get();
        this.slice = Slices.wrappedBuffer(buffer);
    }

    public List<Slice> getSlices()
    {
        return ImmutableList.<Slice>builder()
                .addAll(closedSlices)
                .add(Slices.copyOf(slice, 0, bufferPosition))
                .build();
    }

    @Override
    public void reset()
    {
        chunkSupplier.reset();
        closedSlices.clear();

        buffer = chunkSupplier.get();
        slice = Slices.wrappedBuffer(buffer);

        closedSlicesRetainedSize = 0;
        streamOffset = 0;
        bufferPosition = 0;
    }

    @Override
    public void reset(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        return toIntExact(streamOffset + bufferPosition);
    }

    @Override
    public long getRetainedSize()
    {
        return slice.getRetainedSize() + closedSlicesRetainedSize + INSTANCE_SIZE;
    }

    @Override
    public int writableBytes()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isWritable()
    {
        return true;
    }

    @Override
    public void writeByte(int value)
    {
        ensureWritableBytes(SIZE_OF_BYTE);
        slice.setByte(bufferPosition, value);
        bufferPosition += SIZE_OF_BYTE;
    }

    @Override
    public void writeShort(int value)
    {
        ensureWritableBytes(SIZE_OF_SHORT);
        slice.setShort(bufferPosition, value);
        bufferPosition += SIZE_OF_SHORT;
    }

    @Override
    public void writeInt(int value)
    {
        ensureWritableBytes(SIZE_OF_INT);
        slice.setInt(bufferPosition, value);
        bufferPosition += SIZE_OF_INT;
    }

    @Override
    public void writeLong(long value)
    {
        ensureWritableBytes(SIZE_OF_LONG);
        slice.setLong(bufferPosition, value);
        bufferPosition += SIZE_OF_LONG;
    }

    @Override
    public void writeFloat(float value)
    {
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value)
    {
        writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void writeBytes(Slice source)
    {
        writeBytes(source, 0, source.length());
    }

    @Override
    public void writeBytes(Slice source, int sourceIndex, int length)
    {
        while (length > 0) {
            int batch = tryEnsureBatchSize(length);
            slice.setBytes(bufferPosition, source, sourceIndex, batch);
            bufferPosition += batch;
            sourceIndex += batch;
            length -= batch;
        }
    }

    @Override
    public void writeBytes(byte[] source)
    {
        writeBytes(source, 0, source.length);
    }

    @Override
    public void writeBytes(byte[] source, int sourceIndex, int length)
    {
        while (length > 0) {
            int batch = tryEnsureBatchSize(length);
            slice.setBytes(bufferPosition, source, sourceIndex, batch);
            bufferPosition += batch;
            sourceIndex += batch;
            length -= batch;
        }
    }

    @Override
    public void writeBytes(InputStream in, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = tryEnsureBatchSize(length);
            slice.setBytes(bufferPosition, in, batch);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public void writeZero(int length)
    {
        checkArgument(length >= 0, "length must be greater than or equal to 0");

        while (length > 0) {
            int batch = tryEnsureBatchSize(length);
            Arrays.fill(buffer, bufferPosition, bufferPosition + batch, (byte) 0);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public SliceOutput appendLong(long value)
    {
        writeLong(value);
        return this;
    }

    @Override
    public SliceOutput appendDouble(double value)
    {
        writeDouble(value);
        return this;
    }

    @Override
    public SliceOutput appendInt(int value)
    {
        writeInt(value);
        return this;
    }

    @Override
    public SliceOutput appendShort(int value)
    {
        writeShort(value);
        return this;
    }

    @Override
    public SliceOutput appendByte(int value)
    {
        writeByte(value);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source, int sourceIndex, int length)
    {
        writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public SliceOutput appendBytes(byte[] source)
    {
        writeBytes(source);
        return this;
    }

    @Override
    public SliceOutput appendBytes(Slice slice)
    {
        writeBytes(slice);
        return this;
    }

    @Override
    public Slice slice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getUnderlyingSlice()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(Charset charset)
    {
        return toString();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("OutputStreamSliceOutputAdapter{");
        builder.append("position=").append(size());
        builder.append("bufferSize=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    private int tryEnsureBatchSize(int length)
    {
        ensureWritableBytes(min(MAX_UNUSED_BUFFER_SIZE, length));
        return min(length, slice.length() - bufferPosition);
    }

    private void ensureWritableBytes(int minWritableBytes)
    {
        checkArgument(minWritableBytes <= MAX_UNUSED_BUFFER_SIZE);
        if (bufferPosition + minWritableBytes > slice.length()) {
            closeChunk();
        }
    }

    private void closeChunk()
    {
        // add trimmed view of slice to closed slices
        closedSlices.add(slice.slice(0, bufferPosition));
        closedSlicesRetainedSize += slice.getRetainedSize();

        // create a new buffer
        // double size until we hit the max chunk size
        buffer = chunkSupplier.get();
        slice = Slices.wrappedBuffer(buffer);

        streamOffset += bufferPosition;
        bufferPosition = 0;
    }

    // Chunk supplier creates buffers by doubling the size from min to max chunk size.
    // The supplier also tracks all created buffers and can be reset to the beginning,
    // reusing the buffers.
    private static class ChunkSupplier
    {
        private final int maxChunkSize;

        private final List<byte[]> bufferPool = new ArrayList<>();
        private final List<byte[]> usedBuffers = new ArrayList<>();

        private int currentSize;

        public ChunkSupplier(int minChunkSize, int maxChunkSize)
        {
            checkArgument(minChunkSize >= MINIMUM_CHUNK_SIZE, "minimum chunk size of " + MINIMUM_CHUNK_SIZE + " required");
            checkArgument(maxChunkSize <= MAXIMUM_CHUNK_SIZE, "maximum chunk size of " + MAXIMUM_CHUNK_SIZE + " required");
            checkArgument(minChunkSize <= maxChunkSize, "minimum chunk size must be less than maximum chunk size");

            this.currentSize = minChunkSize;
            this.maxChunkSize = maxChunkSize;
        }

        public void reset()
        {
            bufferPool.addAll(0, usedBuffers);
            usedBuffers.clear();
        }

        public byte[] get()
        {
            byte[] buffer;
            if (bufferPool.isEmpty()) {
                currentSize = min(multiplyExact(currentSize, 2), maxChunkSize);
                buffer = new byte[currentSize];
            }
            else {
                buffer = bufferPool.remove(0);
                currentSize = buffer.length;
            }
            usedBuffers.add(buffer);
            return buffer;
        }
    }
}
