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
import com.facebook.presto.orc.OrcDecompressor.OutputBuffer;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Arrays;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class CompressedOrcChunkLoader
        implements OrcChunkLoader
{
    private final OrcDataReader dataReader;
    private final LocalMemoryContext dataReaderMemoryUsage;
    private final OrcDecompressor decompressor;
    private final LocalMemoryContext decompressionBufferMemoryUsage;

    private FixedLengthSliceInput compressedBufferStream = EMPTY_SLICE.getInput();
    private int compressedBufferStart;
    private int nextUncompressedOffset;
    private long lastCheckpoint;

    private byte[] decompressorOutputBuffer;

    public CompressedOrcChunkLoader(
            OrcDataReader dataReader,
            OrcDecompressor decompressor,
            AggregatedMemoryContext memoryContext)
    {
        this.dataReader = requireNonNull(dataReader, "dataReader is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        requireNonNull(memoryContext, "memoryContext is null");
        this.dataReaderMemoryUsage = memoryContext.newLocalMemoryContext(CompressedOrcChunkLoader.class.getSimpleName());
        dataReaderMemoryUsage.setBytes(dataReader.getRetainedSize());
        this.decompressionBufferMemoryUsage = memoryContext.newLocalMemoryContext(CompressedOrcChunkLoader.class.getSimpleName());
    }

    @Override
    public OrcDataSourceId getOrcDataSourceId()
    {
        return dataReader.getOrcDataSourceId();
    }

    private int getCurrentCompressedOffset()
    {
        return toIntExact(compressedBufferStart + compressedBufferStream.position());
    }

    @Override
    public boolean hasNextChunk()
    {
        return getCurrentCompressedOffset() < dataReader.getSize();
    }

    @Override
    public long getLastCheckpoint()
    {
        return lastCheckpoint;
    }

    @Override
    public void seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedOffset = decodeCompressedBlockOffset(checkpoint);
        if (compressedOffset >= dataReader.getSize()) {
            throw new OrcCorruptionException(dataReader.getOrcDataSourceId(), "Seek past end of stream");
        }
        // is the compressed offset within the current compressed buffer
        if (compressedBufferStart <= compressedOffset && compressedOffset < compressedBufferStart + compressedBufferStream.length()) {
            compressedBufferStream.setPosition(compressedOffset - compressedBufferStart);
        }
        else {
            compressedBufferStart = compressedOffset;
            compressedBufferStream = EMPTY_SLICE.getInput();
        }

        nextUncompressedOffset = decodeDecompressedOffset(checkpoint);
        lastCheckpoint = checkpoint;
    }

    @Override
    public Slice nextChunk()
            throws IOException
    {
        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        ensureCompressedBytesAvailable(3);
        lastCheckpoint = createInputStreamCheckpoint(getCurrentCompressedOffset(), nextUncompressedOffset);
        int b0 = compressedBufferStream.readUnsignedByte();
        int b1 = compressedBufferStream.readUnsignedByte();
        int b2 = compressedBufferStream.readUnsignedByte();

        boolean isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);

        ensureCompressedBytesAvailable(chunkLength);
        Slice chunk = compressedBufferStream.readSlice(chunkLength);

        if (!isUncompressed) {
            int uncompressedSize = decompressor.decompress(
                    (byte[]) chunk.getBase(),
                    (int) (chunk.getAddress() - ARRAY_BYTE_BASE_OFFSET),
                    chunk.length(),
                    createOutputBuffer());
            chunk = Slices.wrappedBuffer(decompressorOutputBuffer, 0, uncompressedSize);
        }
        if (nextUncompressedOffset != 0) {
            chunk = chunk.slice(nextUncompressedOffset, chunk.length() - nextUncompressedOffset);
            nextUncompressedOffset = 0;
            // if we positioned to the end of the chunk, read the next one
            if (chunk.length() == 0) {
                chunk = nextChunk();
            }
        }
        return chunk;
    }

    private void ensureCompressedBytesAvailable(int size)
            throws IOException
    {
        // is this within the current buffer?
        if (size <= compressedBufferStream.remaining()) {
            return;
        }

        // is this a read larger than the buffer
        if (size > dataReader.getMaxBufferSize()) {
            throw new OrcCorruptionException(dataReader.getOrcDataSourceId(), "Requested read size (%s bytes) is greater than max buffer size (%s bytes", size, dataReader.getMaxBufferSize());
        }

        // is this a read past the end of the stream
        if (compressedBufferStart + compressedBufferStream.position() + size > dataReader.getSize()) {
            throw new OrcCorruptionException(dataReader.getOrcDataSourceId(), "Read past end of stream");
        }

        compressedBufferStart = compressedBufferStart + toIntExact(compressedBufferStream.position());
        Slice compressedBuffer = dataReader.seekBuffer(compressedBufferStart);
        dataReaderMemoryUsage.setBytes(dataReader.getRetainedSize());
        if (compressedBuffer.length() < size) {
            throw new OrcCorruptionException(dataReader.getOrcDataSourceId(), "Requested read of %s bytes but only %s were bytes", size, compressedBuffer.length());
        }
        compressedBufferStream = compressedBuffer.getInput();
    }

    private OutputBuffer createOutputBuffer()
    {
        return new OutputBuffer()
        {
            @Override
            public byte[] initialize(int size)
            {
                if (decompressorOutputBuffer == null || size > decompressorOutputBuffer.length) {
                    decompressorOutputBuffer = new byte[size];
                    decompressionBufferMemoryUsage.setBytes(decompressorOutputBuffer.length);
                }
                return decompressorOutputBuffer;
            }

            @Override
            public byte[] grow(int size)
            {
                if (size > decompressorOutputBuffer.length) {
                    decompressorOutputBuffer = Arrays.copyOfRange(decompressorOutputBuffer, 0, size);
                    decompressionBufferMemoryUsage.setBytes(decompressorOutputBuffer.length);
                }
                return decompressorOutputBuffer;
            }
        };
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("loader", dataReader)
                .add("compressedOffset", getCurrentCompressedOffset())
                .add("decompressor", decompressor)
                .toString();
    }
}
