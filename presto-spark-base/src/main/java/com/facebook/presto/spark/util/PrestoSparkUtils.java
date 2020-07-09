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

import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.zip.DeflaterInputStream;
import java.util.zip.InflaterOutputStream;

import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.ByteStreams.toByteArray;

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
                prestoSparkSerializedPage.getPositionCount(),
                prestoSparkSerializedPage.getUncompressedSizeInBytes());
    }

    public static <T> Iterator<T> getNullifyingIterator(List<T> list)
    {
        return new AbstractIterator<T>()
        {
            private int index;

            @Override
            protected T computeNext()
            {
                if (index >= list.size()) {
                    return endOfData();
                }
                T element = list.get(index);
                list.set(index, null);
                index++;
                return element;
            }
        };
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
}
