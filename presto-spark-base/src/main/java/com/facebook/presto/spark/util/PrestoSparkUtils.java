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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterOutputStream;

import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.toByteArray;
import static java.lang.Math.toIntExact;

public class PrestoSparkUtils
{
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
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
