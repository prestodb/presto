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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.google.common.collect.PeekingIterator;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class OrcInputStream
        extends InputStream implements PeekingIterator<OrcInputStream.Buffer>
{
    private final OrcDataSourceId orcDataSourceId;
    private final FixedLengthSliceInput compressedSliceInput;
    private final Optional<OrcDecompressor> decompressor;

    private int currentCompressedBlockOffset;

    private byte[] buffer;
    private int position;
    private int length;

    private final LocalMemoryContext bufferMemoryUsage;
    private boolean isUncompressed;
    private int uncompressedOffset;
    private Buffer bufferContainer = new Buffer();

    public OrcInputStream(
            OrcDataSourceId orcDataSourceId,
            FixedLengthSliceInput sliceInput,
            Optional<OrcDecompressor> decompressor,
            AggregatedMemoryContext systemMemoryContext,
            long sliceInputRetainedSizeInBytes)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSource is null");

        requireNonNull(sliceInput, "sliceInput is null");

        this.decompressor = requireNonNull(decompressor, "decompressor is null");

        // memory reserved in the systemMemoryContext is never release and instead it is
        // expected that the context itself will be destroyed at the end of the read
        requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.bufferMemoryUsage = systemMemoryContext.newLocalMemoryContext(OrcInputStream.class.getSimpleName());
        checkArgument(sliceInputRetainedSizeInBytes >= 0, "sliceInputRetainedSizeInBytes is negative");
        systemMemoryContext.newLocalMemoryContext(OrcInputStream.class.getSimpleName()).setBytes(sliceInputRetainedSizeInBytes);

        if (!decompressor.isPresent()) {
            long sliceInputPosition = sliceInput.position();
            this.buffer = new byte[toIntExact(sliceInput.remaining())];
            this.length = buffer.length;
            sliceInput.readFully(buffer, toIntExact(sliceInput.position()), toIntExact(sliceInput.remaining()));
            sliceInput.setPosition(sliceInputPosition);
            this.compressedSliceInput = EMPTY_SLICE.getInput();
        }
        else {
            this.compressedSliceInput = sliceInput;
            this.buffer = new byte[0];
        }
    }

    @Override
    public void close()
    {
        // close may be called to unpin cache or pool resources.
        compressedSliceInput.close();
    }

    @Override
    public int available()
    {
        if (buffer == null) {
            return 0;
        }
        return length - position;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public byte[] getBuffer(int minBytes)
    {
        if (available() < minBytes) {
            return null;
        }
        return buffer;
    }

    public int getOffsetInBuffer()
    {
        return (int) position;
    }

    @Override
    public int read()
            throws IOException
    {
        if (buffer == null) {
            return -1;
        }
        if (available() > 0) {
            return 0xff & buffer[position++];
        }

        advance();
        return read();
    }

    @Override
    public int read(byte[] b, int off, int length)
            throws IOException
    {
        if (buffer == null) {
            return -1;
        }

        if (available() == 0) {
            advance();
            if (buffer == null) {
                return -1;
            }
        }
        length = Math.min(length, available());
        System.arraycopy(buffer, position, b, off, length);
        position += length;
        return length;
    }

    public void skipFully(long length)
            throws IOException
    {
        while (length > 0) {
            long result = skip(length);
            if (result < 0) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected end of stream");
            }
            length -= result;
        }
    }

    public void readFully(byte[] buffer, int offset, int length)
            throws IOException
    {
        while (offset < length) {
            int result = read(buffer, offset, length - offset);
            if (result < 0) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected end of stream");
            }
            offset += result;
        }
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    public long getCheckpoint()
    {
        // if the decompressed buffer is empty, return a checkpoint starting at the next block
        if (buffer == null || (position == 0 && available() == 0)) {
            return createInputStreamCheckpoint(toIntExact(compressedSliceInput.position()), 0);
        }
        // otherwise return a checkpoint at the last compressed block read and the current position in the buffer
        return createInputStreamCheckpoint(currentCompressedBlockOffset, toIntExact(position));
    }

    public boolean seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedBlockOffset = decodeCompressedBlockOffset(checkpoint);
        int decompressedOffset = decodeDecompressedOffset(checkpoint);
        boolean discardedBuffer;
        if (compressedBlockOffset != currentCompressedBlockOffset) {
            if (!decompressor.isPresent()) {
                throw new OrcCorruptionException(orcDataSourceId, "Reset stream has a compressed block offset but stream is not compressed");
            }
            compressedSliceInput.setPosition(compressedBlockOffset);
            buffer = new byte[0];
            position = 0;
            length = 0;
            uncompressedOffset = 0;
            discardedBuffer = true;
        }
        else {
            discardedBuffer = false;
        }

        if (decompressedOffset != position) {
            position = uncompressedOffset;
            if (available() < decompressedOffset) {
                decompressedOffset -= available();
                advance();
            }
            position += decompressedOffset;
        }
        else if (length == 0) {
            advance();
            position += decompressedOffset;
        }
        return discardedBuffer;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (buffer == null || n <= 0) {
            return -1;
        }

        long result = Math.min(available(), n);
        position += toIntExact(result);
        if (result != 0) {
            return result;
        }
        if (read() == -1) {
            return 0;
        }
        result = Math.min(available(), n - 1);
        position += toIntExact(result);
        return 1 + result;
    }

    // This comes from the Apache Hive ORC code
    private void advance()
            throws IOException
    {
        if (compressedSliceInput == null || compressedSliceInput.remaining() == 0) {
            buffer = null;
            position = 0;
            length = 0;
            // So bufferContainer.buffer can be gc'd
            bufferContainer = null;
            uncompressedOffset = 0;
            return;
        }

        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        currentCompressedBlockOffset = toIntExact(compressedSliceInput.position());
        int b0 = compressedSliceInput.readUnsignedByte();
        int b1 = compressedSliceInput.readUnsignedByte();
        int b2 = compressedSliceInput.readUnsignedByte();

        isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);
        if (chunkLength < 0 || chunkLength > compressedSliceInput.remaining()) {
            throw new OrcCorruptionException(orcDataSourceId, "The chunkLength (%s) must not be negative or greater than remaining size (%s)", chunkLength, compressedSliceInput.remaining());
        }

        Slice chunk = compressedSliceInput.readSlice(chunkLength);

        if (bufferContainer == null) {
            bufferContainer = new Buffer();
        }
        if (isUncompressed) {
            buffer = (byte[]) chunk.getBase();
            position = toIntExact(chunk.getAddress() - ARRAY_BYTE_BASE_OFFSET);
            length = toIntExact(position + chunk.length());
        }
        else {
            OrcDecompressor.OutputBuffer output = new OrcDecompressor.OutputBuffer()
            {
                @Override
                public byte[] initialize(int size)
                {
                    if (buffer == null || size > buffer.length) {
                        buffer = new byte[size];
                        bufferMemoryUsage.setBytes(buffer.length);
                        position = 0;
                        length = size;
                    }
                    return buffer;
                }

                @Override
                public byte[] grow(int size)
                {
                    if (size > buffer.length) {
                        buffer = Arrays.copyOfRange(buffer, 0, size);
                        bufferMemoryUsage.setBytes(buffer.length);
                    }
                    return buffer;
                }
            };
            length = decompressor.get().decompress((byte[]) chunk.getBase(), (int) (chunk.getAddress() - ARRAY_BYTE_BASE_OFFSET), chunk.length(), output);
            position = 0;
        }
        uncompressedOffset = position;
    }

    @Override
    public boolean hasNext()
    {
        return buffer != null;
    }

    @Override
    public Buffer next()
    {
        if (buffer == null) {
            throw new NoSuchElementException();
        }
        try {
            if (decompressor.isPresent()) {
                advance();
                if (buffer == null) {
                    currentCompressedBlockOffset = toIntExact(compressedSliceInput.position());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        bufferContainer.setBuffer(buffer);
        bufferContainer.setPosition(position);
        bufferContainer.setLength(length);

        return bufferContainer;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Buffer peek()
    {
        bufferContainer.setBuffer(buffer);
        bufferContainer.setPosition(position);
        bufferContainer.setLength(length);
        return bufferContainer;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", orcDataSourceId)
                .add("compressedOffset", compressedSliceInput.position())
                .add("uncompressedOffset", buffer == null ? null : position)
                .add("decompressor", decompressor.map(Object::toString).orElse("none"))
                .toString();
    }

    public static class Buffer
    {
        private byte[] buffer;
        private int position;
        private int length;

        public byte[] getBuffer()
        {
            return buffer;
        }

        public int getPosition()
        {
            return position;
        }

        public int getLength()
        {
            return length;
        }

        public void setBuffer(byte[] buffer)
        {
            this.buffer = buffer;
        }

        public void setPosition(int position)
        {
            this.position = position;
        }

        public void setLength(int length)
        {
            this.length = length;
        }
    }
}
