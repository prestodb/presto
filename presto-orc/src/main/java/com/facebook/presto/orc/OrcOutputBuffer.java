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
package com.facebook.presto.orc;

import com.facebook.presto.orc.checkpoint.InputStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.zlib.DeflateCompressor;
import com.facebook.presto.orc.zstd.ZstdJniCompressor;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class OrcOutputBuffer
        extends SliceOutput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcOutputBuffer.class).instanceSize();
    private static final int PAGE_HEADER_SIZE = 3; // ORC spec 3 byte header
    private static final int INITIAL_BUFFER_SIZE = 256;
    private static final int DIRECT_FLUSH_SIZE = 32 * 1024;
    private static final int MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE = 4 * 1024;
    private static final int MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE = 1024 * 1024;

    private final int maxBufferSize;

    private final ChunkedSliceOutput compressedOutputStream;

    @Nullable
    private final Compressor compressor;
    private byte[] compressionBuffer = new byte[0];
    private final Optional<DwrfDataEncryptor> dwrfEncryptor;

    private Slice slice;
    private byte[] buffer;

    /**
     * Offset of buffer within stream.
     */
    private long bufferOffset;
    /**
     * Current position for writing in buffer.
     */
    private int bufferPosition;

    public OrcOutputBuffer(CompressionParameters compressionParameters, Optional<DwrfDataEncryptor> dwrfEncryptor)
    {
        requireNonNull(compressionParameters, "compressionParameters is null");
        requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
        int maxBufferSize = compressionParameters.getMaxBufferSize();
        checkArgument(maxBufferSize > PAGE_HEADER_SIZE, "maximum buffer size should be greater than page header size");

        CompressionKind compressionKind = compressionParameters.getKind();
        this.maxBufferSize = compressionKind == CompressionKind.NONE ? maxBufferSize : maxBufferSize - PAGE_HEADER_SIZE;

        this.buffer = new byte[INITIAL_BUFFER_SIZE];
        this.slice = wrappedBuffer(buffer);

        compressedOutputStream = new ChunkedSliceOutput(MINIMUM_OUTPUT_BUFFER_CHUNK_SIZE, MAXIMUM_OUTPUT_BUFFER_CHUNK_SIZE);

        if (compressionKind == CompressionKind.NONE) {
            this.compressor = null;
        }
        else if (compressionKind == CompressionKind.SNAPPY) {
            this.compressor = new SnappyCompressor();
        }
        else if (compressionKind == CompressionKind.ZLIB) {
            this.compressor = new DeflateCompressor(compressionParameters.getLevel());
        }
        else if (compressionKind == CompressionKind.LZ4) {
            this.compressor = new Lz4Compressor();
        }
        else if (compressionKind == CompressionKind.ZSTD) {
            this.compressor = new ZstdJniCompressor(compressionParameters.getLevel());
        }
        else {
            throw new IllegalArgumentException("Unsupported compression " + compressionKind);
        }

        this.dwrfEncryptor = requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");
    }

    public long getOutputDataSize()
    {
        checkState(bufferPosition == 0, "Buffer must be flushed before getOutputDataSize can be called");
        return compressedOutputStream.size();
    }

    public long estimateOutputDataSize()
    {
        return compressedOutputStream.size() + bufferPosition;
    }

    public int writeDataTo(SliceOutput outputStream)
    {
        checkState(bufferPosition == 0, "Buffer must be closed before writeDataTo can be called");
        for (Slice slice : compressedOutputStream.getSlices()) {
            outputStream.writeBytes(slice);
        }
        return compressedOutputStream.size();
    }

    public long getCheckpoint()
    {
        if (compressor == null && !dwrfEncryptor.isPresent()) {
            return size();
        }
        return InputStreamCheckpoint.createInputStreamCheckpoint(compressedOutputStream.size(), bufferPosition);
    }

    @Override
    public void flush()
    {
        flushBufferToOutputStream();
    }

    @Override
    public void close()
    {
        flushBufferToOutputStream();
    }

    @Override
    public void reset()
    {
        compressedOutputStream.reset();
        bufferOffset = 0;
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
        return toIntExact(bufferOffset + bufferPosition);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + compressedOutputStream.getRetainedSize() + slice.getRetainedSize() + SizeOf.sizeOf(compressionBuffer);
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
        // This normalizes NaN values like `java.io.DataOutputStream` does
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value)
    {
        // This normalizes NaN values like `java.io.DataOutputStream` does
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
        // Write huge chunks direct to OutputStream
        if (length >= DIRECT_FLUSH_SIZE) {
            flushBufferToOutputStream();
            writeDirectlyToOutputStream((byte[]) source.getBase(), sourceIndex + (int) (source.getAddress() - ARRAY_BYTE_BASE_OFFSET), length);
            bufferOffset += length;
        }
        else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
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
        // Write huge chunks direct to OutputStream
        if (length >= DIRECT_FLUSH_SIZE) {
            // todo fill buffer before flushing
            flushBufferToOutputStream();
            writeDirectlyToOutputStream(source, sourceIndex, length);
            bufferOffset += length;
        }
        else {
            ensureWritableBytes(length);
            slice.setBytes(bufferPosition, source, sourceIndex, length);
            bufferPosition += length;
        }
    }

    @Override
    public void writeBytes(InputStream in, int length)
            throws IOException
    {
        while (length > 0) {
            int batch = ensureBatchSize(length);
            slice.setBytes(bufferPosition, in, batch);
            bufferPosition += batch;
            length -= batch;
        }
    }

    @Override
    public void writeZero(int length)
    {
        checkArgument(length >= 0, "length must be 0 or greater than 0.");

        while (length > 0) {
            int batch = ensureBatchSize(length);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("OutputStreamSliceOutputAdapter{");
        builder.append("outputStream=").append(compressedOutputStream);
        builder.append("bufferSize=").append(slice.length());
        builder.append('}');
        return builder.toString();
    }

    private void ensureWritableBytes(int minWritableBytes)
    {
        checkArgument(minWritableBytes <= maxBufferSize, "Min writable bytes must not exceed max buffer size");

        int neededBufferSize = bufferPosition + minWritableBytes;
        if (neededBufferSize <= slice.length()) {
            return;
        }

        if (slice.length() >= maxBufferSize) {
            flushBufferToOutputStream();
            return;
        }

        // grow the buffer size
        int newBufferSize = min(max(slice.length() * 2, minWritableBytes), maxBufferSize);
        if (neededBufferSize <= newBufferSize) {
            // we have capacity in the new buffer; just copy the data to the new buffer
            byte[] previousBuffer = buffer;
            buffer = new byte[newBufferSize];
            slice = wrappedBuffer(buffer);
            System.arraycopy(previousBuffer, 0, buffer, 0, bufferPosition);
        }
        else {
            // there is no enough capacity in the new buffer; flush the data and allocate the new buffer
            flushBufferToOutputStream();
            buffer = new byte[newBufferSize];
            slice = wrappedBuffer(buffer);
        }
    }

    private int ensureBatchSize(int length)
    {
        ensureWritableBytes(min(DIRECT_FLUSH_SIZE, length));
        return min(length, slice.length() - bufferPosition);
    }

    private void flushBufferToOutputStream()
    {
        if (bufferPosition > 0) {
            writeChunkToOutputStream(buffer, 0, bufferPosition);
            bufferOffset += bufferPosition;
            bufferPosition = 0;
        }
    }

    private void writeChunkToOutputStream(byte[] chunk, int offset, int length)
    {
        if (compressor == null && !dwrfEncryptor.isPresent()) {
            compressedOutputStream.write(chunk, offset, length);
            return;
        }

        checkArgument(length <= buffer.length, "Write chunk length must be less than compression buffer size");

        boolean isCompressed = false;
        if (compressor != null) {
            int minCompressionBufferSize = compressor.maxCompressedLength(length);
            if (compressionBuffer.length < minCompressionBufferSize) {
                compressionBuffer = new byte[minCompressionBufferSize];
            }
            int compressedSize = compressor.compress(chunk, offset, length, compressionBuffer, 0, compressionBuffer.length);
            if (compressedSize < length) {
                isCompressed = true;
                chunk = compressionBuffer;
                length = compressedSize;
                offset = 0;
            }
        }
        if (dwrfEncryptor.isPresent()) {
            chunk = dwrfEncryptor.get().encrypt(chunk, offset, length);
            length = chunk.length;
            offset = 0;
            // size after encryption should not exceed what the 3 byte header can hold (2^23)
            if (length > 8388608) {
                throw new OrcEncryptionException("Encrypted data size %s exceeds limit of 2^23", length);
            }
        }
        int header = isCompressed ? length << 1 : (length << 1) + 1;

        writeChunkedOutput(chunk, offset, length, header);
    }

    private void writeChunkedOutput(byte[] chunk, int offset, int length, int header)
    {
        compressedOutputStream.write(header & 0x00_00FF);
        compressedOutputStream.write((header & 0x00_FF00) >> 8);
        compressedOutputStream.write((header & 0xFF_0000) >> 16);
        compressedOutputStream.writeBytes(chunk, offset, length);
    }

    private void writeDirectlyToOutputStream(byte[] bytes, int bytesOffset, int length)
    {
        if (compressor == null && !dwrfEncryptor.isPresent()) {
            compressedOutputStream.writeBytes(bytes, bytesOffset, length);
            return;
        }

        while (length > 0) {
            int chunkSize = Integer.min(length, buffer.length);
            writeChunkToOutputStream(bytes, bytesOffset, chunkSize);
            length -= chunkSize;
            bytesOffset += chunkSize;
        }
    }

    @VisibleForTesting
    int getBufferCapacity()
    {
        return slice.length();
    }
}
