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
package com.facebook.presto.spi.page;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.Optional;

import static com.facebook.presto.spi.page.PageCodecMarker.CHECKSUMMED;
import static com.facebook.presto.spi.page.PageCodecMarker.COMPRESSED;
import static com.facebook.presto.spi.page.PageCodecMarker.ENCRYPTED;
import static com.facebook.presto.spi.page.PagesSerdeUtil.computeSerializedPageChecksum;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readRawPage;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeRawPage;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@NotThreadSafe
public class PagesSerde
{
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<PageCompressor> compressor;
    private final Optional<PageDecompressor> decompressor;
    private final Optional<SpillCipher> spillCipher;
    private final boolean checksumEnabled;

    private byte[] compressionBuffer;

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<PageCompressor> compressor, Optional<PageDecompressor> decompressor, Optional<SpillCipher> spillCipher)
    {
        this(blockEncodingSerde, compressor, decompressor, spillCipher, false);
    }

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<PageCompressor> compressor, Optional<PageDecompressor> decompressor, Optional<SpillCipher> spillCipher, boolean checksumEnabled)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        checkArgument(compressor.isPresent() == decompressor.isPresent(), "compressor and decompressor must both be present or both be absent");
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.spillCipher = requireNonNull(spillCipher, "spillCipher is null");
        checkState(!spillCipher.isPresent() || !spillCipher.get().isDestroyed(), "spillCipher is already destroyed");
        this.checksumEnabled = checksumEnabled;
    }

    public SerializedPage serialize(Page page)
    {
        SliceOutput serializationBuffer = new DynamicSliceOutput(toIntExact(page.getSizeInBytes() + Integer.BYTES)); // block length is an int
        writeRawPage(page, serializationBuffer, blockEncodingSerde);

        return wrapSlice(serializationBuffer.slice(), page.getPositionCount());
    }

    public SerializedPage serialize(Slice slice, int positionCount)
    {
        checkArgument(slice.isCompact(), "slice is not compact");
        return wrapSlice(slice, positionCount);
    }

    public Page deserialize(SerializedPage serializedPage)
    {
        checkArgument(serializedPage != null, "serializedPage is null");

        Slice slice = serializedPage.getSlice();

        if (ENCRYPTED.isSet(serializedPage.getPageCodecMarkers())) {
            checkState(spillCipher.isPresent(), "Page is encrypted, but spill cipher is missing");
            slice = Slices.wrappedBuffer(spillCipher.get().decrypt(slice.toByteBuffer()));
        }

        if (COMPRESSED.isSet(serializedPage.getPageCodecMarkers())) {
            checkState(decompressor.isPresent(), "Page is compressed, but decompressor is missing");

            int uncompressedSize = serializedPage.getUncompressedSizeInBytes();
            ByteBuffer decompressionBuffer = ByteBuffer.allocate(uncompressedSize);

            decompressor.get().decompress(slice.toByteBuffer(), decompressionBuffer);
            decompressionBuffer.flip();
            checkState(decompressionBuffer.remaining() == uncompressedSize, "page size changed after decompression into decompressionBuffer");

            slice = Slices.wrappedBuffer(decompressionBuffer);
        }

        return readRawPage(serializedPage.getPositionCount(), slice.getInput(), blockEncodingSerde);
    }

    public long getSizeInBytes()
    {
        return compressionBuffer == null ? 0 : compressionBuffer.length;
    }

    public long getRetainedSizeInBytes()
    {
        return sizeOf(compressionBuffer);
    }

    private SerializedPage wrapSlice(Slice slice, int positionCount)
    {
        int uncompressedSize = slice.length();
        byte markers = PageCodecMarker.none();

        if (compressor.isPresent()) {
            int maxCompressedSize = compressor.get().maxCompressedLength(uncompressedSize);
            compressionBuffer = ensureCapacity(compressionBuffer, maxCompressedSize);
            int compressedSize = compressor.get().compress(
                    (byte[]) slice.getBase(),
                    (int) (slice.getAddress() - ARRAY_BYTE_BASE_OFFSET),
                    uncompressedSize,
                    compressionBuffer,
                    0,
                    maxCompressedSize);

            if (compressedSize / (double) uncompressedSize <= MINIMUM_COMPRESSION_RATIO) {
                slice = Slices.copyOf(Slices.wrappedBuffer(compressionBuffer, 0, compressedSize));
                markers = COMPRESSED.set(markers);
            }
        }

        if (spillCipher.isPresent()) {
            slice = Slices.wrappedBuffer(spillCipher.get().encrypt(slice.toByteBuffer()));
            markers = ENCRYPTED.set(markers);
        }
        else if (!slice.isCompact()) {
            slice = Slices.copyOf(slice);
        }

        long checksum = 0;
        if (checksumEnabled) {
            markers = CHECKSUMMED.set(markers);
            checksum = computeSerializedPageChecksum(slice, markers, positionCount, uncompressedSize);
        }

        return new SerializedPage(slice, markers, positionCount, uncompressedSize, checksum);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void checkState(boolean test, String message)
    {
        if (!test) {
            throw new IllegalStateException(message);
        }
    }

    private static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new byte[capacity];
        }

        return buffer;
    }
}
