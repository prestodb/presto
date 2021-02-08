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

import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.FixedLengthSliceInput;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeCompressedBlockOffset;
import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.decodeDecompressedOffset;
import static com.facebook.presto.orc.stream.LongDecode.zigzagDecode;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.round;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class OrcInputStream
        extends InputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcInputStream.class).instanceSize();

    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final int MAX_VARINT_LENGTH = 10;
    private static final double BUFFER_ALLOWED_MEMORY_WASTE_RATIO = 1.5;

    private final OrcDataSourceId orcDataSourceId;
    private final SharedBuffer sharedDecompressionBuffer;
    private final FixedLengthSliceInput compressedSliceInput;
    private final long compressedSliceInputRetainedSizeInBytes;
    private final Optional<OrcDecompressor> decompressor;
    private final Optional<DwrfDataEncryptor> dwrfDecryptor;
    private final OrcLocalMemoryContext memoryUsage;
    // Temporary memory for reading a float or double at buffer boundary.
    private final byte[] temporaryBuffer = new byte[SIZE_OF_DOUBLE];

    private int currentCompressedBlockOffset;

    private byte[] buffer;
    private int position;
    private int length;
    private int uncompressedOffset;

    public OrcInputStream(
            OrcDataSourceId orcDataSourceId,
            SharedBuffer sharedDecompressionBuffer,
            FixedLengthSliceInput sliceInput,
            Optional<OrcDecompressor> decompressor,
            Optional<DwrfDataEncryptor> dwrfDecryptor,
            OrcAggregatedMemoryContext systemMemoryContext,
            long sliceInputRetainedSizeInBytes)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSource is null");
        this.sharedDecompressionBuffer = requireNonNull(sharedDecompressionBuffer, "sharedDecompressionBuffer is null");

        requireNonNull(sliceInput, "sliceInput is null");

        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.dwrfDecryptor = requireNonNull(dwrfDecryptor, "dwrfDecryptor is null");

        // memory reserved in the systemMemoryContext is never release and instead it is
        // expected that the context itself will be destroyed at the end of the read
        requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.memoryUsage = systemMemoryContext.newOrcLocalMemoryContext(OrcInputStream.class.getSimpleName());

        if (!decompressor.isPresent() && !dwrfDecryptor.isPresent()) {
            // for unencrypted uncompressed input read the entire input and discard the original sliceInput
            int sliceInputPosition = toIntExact(sliceInput.position());
            int sliceInputRemaining = toIntExact(sliceInput.remaining());
            this.buffer = new byte[sliceInputRemaining];
            this.length = buffer.length;
            sliceInput.readFully(buffer, sliceInputPosition, sliceInputRemaining);
            this.compressedSliceInput = EMPTY_SLICE.getInput();
            this.compressedSliceInputRetainedSizeInBytes = compressedSliceInput.getRetainedSize();
        }
        else {
            this.compressedSliceInput = sliceInput;
            this.buffer = new byte[0];
            this.compressedSliceInputRetainedSizeInBytes = sliceInputRetainedSizeInBytes;
        }

        memoryUsage.setBytes(getRetainedSizeInBytes());
    }

    @Override
    public void close()
    {
        // close is never called, so do not add code here
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
        // If we have uncompressed data uncompressedOffset is not included in the offset.
        return createInputStreamCheckpoint(currentCompressedBlockOffset, toIntExact(position - uncompressedOffset));
    }

    public boolean seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedBlockOffset = decodeCompressedBlockOffset(checkpoint);
        int decompressedOffset = decodeDecompressedOffset(checkpoint);
        boolean discardedBuffer;
        if (compressedBlockOffset != currentCompressedBlockOffset) {
            if (!decompressor.isPresent() && !dwrfDecryptor.isPresent()) {
                throw new OrcCorruptionException(orcDataSourceId, "Reset stream has a block offset but stream is not compressed or encrypted");
            }
            compressedSliceInput.setPosition(compressedBlockOffset);
            buffer = new byte[0];
            memoryUsage.setBytes(getRetainedSizeInBytes());
            position = 0;
            length = 0;
            uncompressedOffset = 0;
            discardedBuffer = true;
        }
        else {
            discardedBuffer = false;
        }

        if (decompressedOffset != position - uncompressedOffset) {
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

    public long readDwrfLong(OrcTypeKind type)
            throws IOException
    {
        switch (type) {
            case SHORT:
                return read() | (read() << 8);
            case INT:
                return read() | (read() << 8) | (read() << 16) | (read() << 24);
            case LONG:
                return ((long) read()) |
                        (((long) read()) << 8) |
                        (((long) read()) << 16) |
                        (((long) read()) << 24) |
                        (((long) read()) << 32) |
                        (((long) read()) << 40) |
                        (((long) read()) << 48) |
                        (((long) read()) << 56);
            default:
                throw new IllegalStateException();
        }
    }

    public void skipDwrfLong(OrcTypeKind type, long items)
            throws IOException
    {
        if (items == 0) {
            return;
        }
        long bytes = items;
        switch (type) {
            case SHORT:
                bytes *= SIZE_OF_SHORT;
                break;
            case INT:
                bytes *= SIZE_OF_INT;
                break;
            case LONG:
                bytes *= SIZE_OF_LONG;
                break;
            default:
                throw new IllegalStateException();
        }
        skip(bytes);
    }

    public long readVarint(boolean signed)
            throws IOException
    {
        long result = 0;
        int shift = 0;
        int available = available();
        if (available >= 2 * Long.BYTES) {
            long word = ByteArrays.getLong(buffer, position);
            int count = 1;
            boolean atEnd = false;
            result = word & 0x7f;
            if ((word & 0x80) != 0) {
                long control = word >>> 8;
                long mask = 0x7f << 7;
                while (true) {
                    word = word >>> 1;
                    result |= word & mask;
                    count++;
                    if ((control & 0x80) == 0) {
                        atEnd = true;
                        break;
                    }
                    if (mask == 0x7fL << (7 * 7)) {
                        break;
                    }
                    mask = mask << 7;
                    control = control >>> 8;
                }
                if (!atEnd) {
                    word = ByteArrays.getLong(buffer, position + 8);
                    result |= (word & 0x7f) << 56;
                    if ((word & 0x80) == 0) {
                        count++;
                    }
                    else {
                        result |= 1L << 63;
                        count += 2;
                    }
                }
            }
            position += count;
        }
        else {
            do {
                if (available == 0) {
                    advance();
                    available = available();
                    if (available == 0) {
                        throw new OrcCorruptionException(orcDataSourceId, "End of stream in RLE Integer");
                    }
                }
                available--;
                result |= (long) (buffer[position] & 0x7f) << shift;
                shift += 7;
            }
            while ((buffer[position++] & 0x80) != 0);
        }
        if (signed) {
            return zigzagDecode(result);
        }
        else {
            return result;
        }
    }

    public void skipVarints(long items)
            throws IOException
    {
        if (items == 0) {
            return;
        }

        while (items > 0) {
            items -= skipVarintsInBuffer(items);
        }
    }

    private long skipVarintsInBuffer(long items)
            throws IOException
    {
        if (available() == 0) {
            advance();
            if (available() == 0) {
                throw new OrcCorruptionException(orcDataSourceId, "Unexpected end of stream");
            }
        }
        long skipped = 0;
        // If items to skip is > SIZE_OF_LONG it is safe to skip entire longs
        while (items - skipped > SIZE_OF_LONG && available() > MAX_VARINT_LENGTH) {
            long value = ByteArrays.getLong(buffer, position);
            position += SIZE_OF_LONG;
            long mask = (value & VARINT_MASK) ^ VARINT_MASK;
            skipped += Long.bitCount(mask);
        }
        while (skipped < items && available() > 0) {
            if ((buffer[position++] & 0x80) == 0) {
                skipped++;
            }
        }
        return skipped;
    }

    public double readDouble()
            throws IOException
    {
        int readPosition = ensureContiguousBytesAndAdvance(SIZE_OF_DOUBLE);
        if (readPosition < 0) {
            return ByteArrays.getDouble(temporaryBuffer, 0);
        }
        return ByteArrays.getDouble(buffer, readPosition);
    }

    public float readFloat()
            throws IOException
    {
        int readPosition = ensureContiguousBytesAndAdvance(SIZE_OF_FLOAT);
        if (readPosition < 0) {
            return ByteArrays.getFloat(temporaryBuffer, 0);
        }
        return ByteArrays.getFloat(buffer, readPosition);
    }

    private int ensureContiguousBytesAndAdvance(int bytes)
            throws IOException
    {
        // If there are numBytes in the buffer, return the offset of the start and advance by numBytes. If not, copy numBytes
        // into temporaryBuffer, advance by numBytes and return -1.
        if (available() >= bytes) {
            int startPosition = position;
            position += bytes;
            return startPosition;
        }
        readFully(temporaryBuffer, 0, bytes);
        return -1;
    }

    // This comes from the Apache Hive ORC code
    private void advance()
            throws IOException
    {
        if (compressedSliceInput == null || compressedSliceInput.remaining() == 0) {
            buffer = null;
            position = 0;
            length = 0;
            uncompressedOffset = 0;
            memoryUsage.setBytes(getRetainedSizeInBytes());
            return;
        }

        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        currentCompressedBlockOffset = toIntExact(compressedSliceInput.position());
        int b0 = compressedSliceInput.readUnsignedByte();
        int b1 = compressedSliceInput.readUnsignedByte();
        int b2 = compressedSliceInput.readUnsignedByte();

        boolean isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);
        if (chunkLength < 0 || chunkLength > compressedSliceInput.remaining()) {
            throw new OrcCorruptionException(orcDataSourceId, "The chunkLength (%s) must not be negative or greater than remaining size (%s)", chunkLength, compressedSliceInput.remaining());
        }

        if (isUncompressed) {
            buffer = ensureCapacity(buffer, chunkLength);
            length = compressedSliceInput.read(buffer, 0, chunkLength);
            if (dwrfDecryptor.isPresent()) {
                buffer = dwrfDecryptor.get().decrypt(buffer, 0, chunkLength);
                length = buffer.length;
            }
            position = 0;
        }
        else {
            sharedDecompressionBuffer.ensureCapacity(chunkLength);
            byte[] compressedBuffer = sharedDecompressionBuffer.get();
            int readCompressed = compressedSliceInput.read(compressedBuffer, 0, chunkLength);
            if (dwrfDecryptor.isPresent()) {
                compressedBuffer = dwrfDecryptor.get().decrypt(compressedBuffer, 0, chunkLength);
                readCompressed = compressedBuffer.length;
            }

            OrcDecompressor.OutputBuffer output = new OrcDecompressor.OutputBuffer()
            {
                @Override
                public byte[] initialize(int size)
                {
                    buffer = ensureCapacity(buffer, size);
                    return buffer;
                }

                @Override
                public byte[] grow(int size)
                {
                    if (size > buffer.length) {
                        buffer = Arrays.copyOfRange(buffer, 0, size);
                    }
                    return buffer;
                }
            };
            length = decompressor.get().decompress(compressedBuffer, 0, readCompressed, output);
            position = 0;
        }
        uncompressedOffset = position;
        memoryUsage.setBytes(getRetainedSizeInBytes());
    }

    private long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                compressedSliceInputRetainedSizeInBytes +
                sizeOf(buffer) +
                sizeOf(temporaryBuffer);
    }

    private static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity || buffer.length > round(capacity * BUFFER_ALLOWED_MEMORY_WASTE_RATIO)) {
            return new byte[capacity];
        }

        return buffer;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", orcDataSourceId)
                .add("compressedOffset", compressedSliceInput.position())
                .add("uncompressedOffset", buffer == null ? null : position)
                .add("decompressor", decompressor.map(Object::toString).orElse("none"))
                .add("decryptor", dwrfDecryptor.map(Object::toString).orElse("none"))
                .toString();
    }
}
