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
package com.facebook.presto.spark.util;

import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spi.page.PageCompressor;
import com.facebook.presto.spi.page.PageDecompressor;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.github.luben.zstd.Zstd;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterOutputStream;

import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.toByteArray;
import static java.lang.Math.toIntExact;

public class PrestoSparkUtils
{
    private static final int COMPRESSION_LEVEL = 3; // default level

    private PrestoSparkUtils() {}

    public static PrestoSparkSerializedPage toPrestoSparkSerializedPage(SerializedPage serializedPage)
    {
        Slice slice = serializedPage.getSlice();
        checkArgument(slice.hasByteArray(), "slice is expected to be based on a byte array");
        return new PrestoSparkSerializedPage(
                compactArray(slice.byteArray(), slice.byteArrayOffset(), slice.length()),
                serializedPage.getPositionCount(),
                serializedPage.getUncompressedSizeInBytes(),
                serializedPage.getPageCodecMarkers());
    }

    public static SerializedPage toSerializedPage(PrestoSparkSerializedPage prestoSparkSerializedPage)
    {
        return new SerializedPage(
                Slices.wrappedBuffer(prestoSparkSerializedPage.getBytes()),
                prestoSparkSerializedPage.getPageCodecMarkers(),
                toIntExact(prestoSparkSerializedPage.getPositionCount()),
                prestoSparkSerializedPage.getUncompressedSizeInBytes());
    }

    public static byte[] compress(byte[] bytes)
    {
        try (DeflaterInputStream decompressor = new DeflaterInputStream(new ByteArrayInputStream(bytes))) {
            return toByteArray(decompressor);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static byte[] decompress(byte[] bytes)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (InflaterOutputStream compressor = new InflaterOutputStream(output)) {
            compressor.write(bytes);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return output.toByteArray();
    }

    public static PagesSerde createPagesSerde(BlockEncodingManager blockEncodingManager)
    {
        return new PagesSerde(
                blockEncodingManager,
                Optional.of(createPageCompressor()),
                Optional.of(createPageDecompressor()),
                Optional.empty());
    }

    private static PageCompressor createPageCompressor()
    {
        // based on ZstdJniCompressor
        return new PageCompressor()
        {
            @Override
            public int maxCompressedLength(int uncompressedSize)
            {
                return toIntExact(Zstd.compressBound(uncompressedSize));
            }

            @Override
            public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            {
                long size = Zstd.compressByteArray(output, outputOffset, maxOutputLength, input, inputOffset, inputLength, COMPRESSION_LEVEL);
                if (Zstd.isError(size)) {
                    throw new RuntimeException(Zstd.getErrorName(size));
                }
                return toIntExact(size);
            }

            @Override
            public void compress(ByteBuffer input, ByteBuffer output)
            {
                if (input.isDirect() || output.isDirect() || !input.hasArray() || !output.hasArray()) {
                    throw new IllegalArgumentException("Non-direct byte buffer backed by byte array required");
                }
                int inputOffset = input.arrayOffset() + input.position();
                int outputOffset = output.arrayOffset() + output.position();

                int written = compress(input.array(), inputOffset, input.remaining(), output.array(), outputOffset, output.remaining());
                output.position(output.position() + written);
            }
        };
    }

    private static PageDecompressor createPageDecompressor()
    {
        // based on ZstdJniDecompressor
        return new PageDecompressor()
        {
            @Override
            public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            {
                long size = Zstd.decompressByteArray(output, 0, maxOutputLength, input, inputOffset, inputLength);
                if (Zstd.isError(size)) {
                    String errorName = Zstd.getErrorName(size);
                    throw new RuntimeException("Zstd JNI decompressor failed with " + errorName);
                }
                return toIntExact(size);
            }

            @Override
            public void decompress(ByteBuffer input, ByteBuffer output)
            {
                if (input.isDirect() || output.isDirect() || !input.hasArray() || !output.hasArray()) {
                    throw new IllegalArgumentException("Non-direct byte buffer backed by byte array required");
                }
                int inputOffset = input.arrayOffset() + input.position();
                int outputOffset = output.arrayOffset() + output.position();

                int written = decompress(input.array(), inputOffset, input.remaining(), output.array(), outputOffset, output.remaining());
                output.position(output.position() + written);
            }
        };
    }
}
