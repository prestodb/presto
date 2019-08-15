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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spiller.SpillCipher;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.Optional;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.execution.buffer.PageCodecMarker.COMPRESSED;
import static com.facebook.presto.execution.buffer.PageCodecMarker.ENCRYPTED;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readRawPage;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writeRawPage;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

@NotThreadSafe
public class PagesSerde
{
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<Compressor> compressor;
    private final Optional<Decompressor> decompressor;
    private final Optional<SpillCipher> spillCipher;

    private byte[] compressionBuffer;

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<Compressor> compressor, Optional<Decompressor> decompressor, Optional<SpillCipher> spillCipher)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        checkArgument(compressor.isPresent() == decompressor.isPresent(), "compressor and decompressor must both be present or both be absent");
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.spillCipher = requireNonNull(spillCipher, "spillCipher is null");
        checkState(!spillCipher.isPresent() || !spillCipher.get().isDestroyed(), "spillCipher is already destroyed");
    }

    public SerializedPage serialize(Page page)
    {
        SliceOutput serializationBuffer = new DynamicSliceOutput(toIntExact(page.getSizeInBytes() + Integer.BYTES)); // block length is an int
        writeRawPage(page, serializationBuffer, blockEncodingSerde);
        Slice slice = serializationBuffer.slice();

        int uncompressedSize = serializationBuffer.size();
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
        else {
            //  Encryption disabled, slice data is likely much smaller than its backing buffer
            //  either because of compression or dynamic sizing of the initial output slice
            slice = Slices.copyOf(slice);
        }

        return new SerializedPage(slice, markers, page.getPositionCount(), uncompressedSize);
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
            checkState(decompressionBuffer.remaining() == uncompressedSize);

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
}
