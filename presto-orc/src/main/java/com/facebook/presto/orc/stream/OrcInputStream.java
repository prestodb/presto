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
import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.memory.LocalMemoryContext;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.iq80.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class OrcInputStream
        extends InputStream
{
    public static final int EXPECTED_COMPRESSION_RATIO = 5;
    private final String source;
    private final FixedLengthSliceInput compressedSliceInput;
    private final CompressionKind compressionKind;
    private final int maxBufferSize;

    private int currentCompressedBlockOffset;
    private FixedLengthSliceInput current;

    private byte[] buffer;
    private final LocalMemoryContext bufferMemoryUsage;

    // When uncompressed,
    // * This tracks the memory usage of `current`.
    // When compressed,
    // * This tracks the memory usage of compressedSliceInput.
    // * Memory pointed to by `current` is always part of `buffer`. It shouldn't be counted again.
    private final LocalMemoryContext fixedMemoryUsage;

    public OrcInputStream(String source, FixedLengthSliceInput sliceInput, CompressionKind compressionKind, int bufferSize, AbstractAggregatedMemoryContext systemMemoryContext)
    {
        this.source = requireNonNull(source, "source is null");

        requireNonNull(sliceInput, "sliceInput is null");

        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
        this.maxBufferSize = bufferSize;

        requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.bufferMemoryUsage = systemMemoryContext.newLocalMemoryContext();
        this.fixedMemoryUsage = systemMemoryContext.newLocalMemoryContext();
        this.fixedMemoryUsage.setBytes(sliceInput.length());

        if (compressionKind == UNCOMPRESSED) {
            this.current = sliceInput;
            this.compressedSliceInput = EMPTY_SLICE.getInput();
        }
        else {
            checkArgument(compressionKind == SNAPPY || compressionKind == ZLIB, "%s compression not supported", compressionKind);
            this.compressedSliceInput = sliceInput;
            this.current = EMPTY_SLICE.getInput();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        current = null;
        fixedMemoryUsage.setBytes(compressedSliceInput.length()); // see comments above for fixedMemoryUsage

        buffer = null;
        bufferMemoryUsage.setBytes(0);
    }

    @Override
    public int available()
            throws IOException
    {
        if (current == null) {
            return 0;
        }
        return current.available();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        int result = current.read();
        if (result != -1) {
            return result;
        }

        advance();
        return read();
    }

    @Override
    public int read(byte[] b, int off, int length)
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        if (current.remaining() == 0) {
            advance();
            if (current == null) {
                return -1;
            }
        }

        return current.read(b, off, length);
    }

    public long getCheckpoint()
    {
        // if the decompressed buffer is empty, return a checkpoint starting at the next block
        if (current == null || (current.position() == 0 && current.remaining() == 0)) {
            return createInputStreamCheckpoint(Ints.checkedCast(compressedSliceInput.position()), 0);
        }
        // otherwise return a checkpoint at the last compressed block read and the current position in the buffer
        return createInputStreamCheckpoint(currentCompressedBlockOffset, Ints.checkedCast(current.position()));
    }

    public boolean seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedBlockOffset = decodeCompressedBlockOffset(checkpoint);
        int decompressedOffset = decodeDecompressedOffset(checkpoint);
        boolean discardedBuffer;
        if (compressedBlockOffset != currentCompressedBlockOffset) {
            if (compressionKind == UNCOMPRESSED) {
                throw new OrcCorruptionException("Reset stream has a compressed block offset but stream is not compressed");
            }
            compressedSliceInput.setPosition(compressedBlockOffset);
            current = EMPTY_SLICE.getInput();
            discardedBuffer = true;
        }
        else {
            discardedBuffer = false;
        }

        if (decompressedOffset != current.position()) {
            current.setPosition(0);
            if (current.remaining() < decompressedOffset) {
                decompressedOffset -= current.remaining();
                advance();
            }
            current.setPosition(decompressedOffset);
        }
        return discardedBuffer;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (current == null || n <= 0) {
            return -1;
        }

        long result = current.skip(n);
        if (result != 0) {
            return result;
        }
        if (read() == -1) {
            return 0;
        }
        return 1 + current.skip(n - 1);
    }

    // This comes from the Apache Hive ORC code
    private void advance()
            throws IOException
    {
        if (compressedSliceInput == null || compressedSliceInput.remaining() == 0) {
            current = null;
            return;
        }

        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        currentCompressedBlockOffset = Ints.checkedCast(compressedSliceInput.position());
        int b0 = compressedSliceInput.readUnsignedByte();
        int b1 = compressedSliceInput.readUnsignedByte();
        int b2 = compressedSliceInput.readUnsignedByte();

        boolean isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);
        if (chunkLength < 0 || chunkLength > compressedSliceInput.remaining()) {
            throw new OrcCorruptionException(String.format("The chunkLength (%s) must not be negative or greater than remaining size (%s)", chunkLength, compressedSliceInput.remaining()));
        }

        Slice chunk = compressedSliceInput.readSlice(chunkLength);

        if (isUncompressed) {
            current = chunk.getInput();
        }
        else {
            int uncompressedSize;
            if (compressionKind == ZLIB) {
                uncompressedSize = decompressZip(chunk);
            }
            else {
                uncompressedSize = decompressSnappy(chunk);
            }

            current = Slices.wrappedBuffer(buffer, 0, uncompressedSize).getInput();
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("source", source)
                .add("compressedOffset", compressedSliceInput.position())
                .add("uncompressedOffset", current == null ? null : current.position())
                .add("compression", compressionKind)
                .toString();
    }

    // This comes from the Apache Hive ORC code
    private int decompressZip(Slice in)
            throws IOException
    {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput((byte[]) in.getBase(), (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET), in.length());
            allocateOrGrowBuffer(in.length() * EXPECTED_COMPRESSION_RATIO, false);
            int uncompressedLength = 0;
            while (true) {
                uncompressedLength += inflater.inflate(buffer, uncompressedLength, buffer.length - uncompressedLength);
                if (inflater.finished() || buffer.length >= maxBufferSize) {
                    break;
                }
                int oldBufferSize = buffer.length;
                allocateOrGrowBuffer(buffer.length * 2, true);
                if (buffer.length <= oldBufferSize) {
                    throw new IllegalStateException(String.format("Buffer failed to grow. Old size %d, current size %d", oldBufferSize, buffer.length));
                }
            }

            if (!inflater.finished()) {
                throw new OrcCorruptionException("Could not decompress all input (output buffer too small?)");
            }

            return uncompressedLength;
        }
        catch (DataFormatException e) {
            throw new OrcCorruptionException(e, "Invalid compressed stream");
        }
        finally {
            inflater.end();
        }
    }

    private int decompressSnappy(Slice in)
            throws IOException
    {
        byte[] inArray = (byte[]) in.getBase();
        int inOffset = (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = in.length();

        int uncompressedLength = Snappy.getUncompressedLength(inArray, inOffset);
        checkArgument(uncompressedLength <= maxBufferSize, "Snappy requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
        allocateOrGrowBuffer(uncompressedLength, false);

        return Snappy.uncompress(inArray, inOffset, inLength, buffer, 0);
    }

    private void allocateOrGrowBuffer(int size, boolean copyExistingData)
    {
        if (buffer == null || buffer.length < size) {
            if (copyExistingData && buffer != null) {
                buffer = Arrays.copyOfRange(buffer, 0, Math.min(size, maxBufferSize));
            }
            else {
                buffer = new byte[Math.min(size, maxBufferSize)];
            }
        }
        bufferMemoryUsage.setBytes(buffer.length);
    }
}
