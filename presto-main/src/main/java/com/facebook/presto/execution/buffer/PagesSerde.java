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
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Optional;

import static com.facebook.presto.execution.buffer.PageCompression.COMPRESSED;
import static com.facebook.presto.execution.buffer.PageCompression.UNCOMPRESSED;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readRawPage;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writeRawPage;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.compress.lz4.Lz4RawCompressor.maxCompressedLength;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PagesSerde
{
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

    private final BlockEncodingSerde blockEncodingSerde;
    private final Optional<Compressor> compressor;
    private final Optional<Decompressor> decompressor;

    public PagesSerde(BlockEncodingSerde blockEncodingSerde, Optional<Compressor> compressor, Optional<Decompressor> decompressor)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        checkArgument(compressor.isPresent() == decompressor.isPresent(), "compressor and decompressor must both be present or both be absent");
    }

    public SerializedPage serialize(Page page)
    {
        SliceOutput serializationBuffer = new DynamicSliceOutput(toIntExact((page.getSizeInBytes() + Integer.BYTES))); // block length is an int
        writeRawPage(page, serializationBuffer, blockEncodingSerde);

        if (!compressor.isPresent()) {
            return new SerializedPage(serializationBuffer.slice(), UNCOMPRESSED, page.getPositionCount(), serializationBuffer.size());
        }

        int maxCompressedLength = maxCompressedLength(serializationBuffer.size());
        byte[] compressionBuffer = new byte[maxCompressedLength];
        int actualCompressedLength = compressor.get().compress(serializationBuffer.slice().getBytes(), 0, serializationBuffer.size(), compressionBuffer, 0, maxCompressedLength);

        if (((1.0 * actualCompressedLength) / serializationBuffer.size()) > MINIMUM_COMPRESSION_RATIO) {
            return new SerializedPage(serializationBuffer.slice(), UNCOMPRESSED, page.getPositionCount(), serializationBuffer.size());
        }

        return new SerializedPage(
                Slices.copyOf(Slices.wrappedBuffer(compressionBuffer, 0, actualCompressedLength)),
                COMPRESSED,
                page.getPositionCount(),
                serializationBuffer.size());
    }

    public Page deserialize(SerializedPage serializedPage)
    {
        checkArgument(serializedPage != null, "serializedPage is null");

        if (!decompressor.isPresent() || serializedPage.getCompression() == UNCOMPRESSED) {
            return readRawPage(serializedPage.getPositionCount(), serializedPage.getSlice().getInput(), blockEncodingSerde);
        }

        int uncompressedSize = serializedPage.getUncompressedSizeInBytes();
        byte[] decompressed = new byte[uncompressedSize];
        int actualUncompressedSize = decompressor.get().decompress(serializedPage.getSlice().getBytes(), 0, serializedPage.getSlice().length(), decompressed, 0, uncompressedSize);
        checkState(uncompressedSize == actualUncompressedSize);

        return readRawPage(serializedPage.getPositionCount(), Slices.wrappedBuffer(decompressed, 0, uncompressedSize).getInput(), blockEncodingSerde);
    }
}
