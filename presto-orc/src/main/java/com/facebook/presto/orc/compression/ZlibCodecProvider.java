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
package com.facebook.presto.orc.compression;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.metadata.CompressionKind;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class ZlibCodecProvider
        implements CodecProvider
{
    private final int maxBufferSize;

    public ZlibCodecProvider(int maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Codec get(AbstractAggregatedMemoryContext memoryContext)
    {
        return new ZlibCodec(maxBufferSize, memoryContext);
    }

    @Override
    public CompressionKind getCompressionKind()
    {
        return CompressionKind.ZLIB;
    }

    private class ZlibCodec
            extends Codec
    {
        public static final int EXPECTED_COMPRESSION_RATIO = 5;

        private final int maxBufferSize;

        public ZlibCodec(int maxBufferSize, AbstractAggregatedMemoryContext systemMemoryContext)
        {
            super(maxBufferSize, systemMemoryContext);
            this.maxBufferSize = maxBufferSize;
        }

        // This comes from the Apache Hive ORC code
        @Override
        public Slice decompress(Slice compressedSlice)
                throws IOException
        {
            Inflater inflater = new Inflater(true);
            try {
                inflater.setInput((byte[]) compressedSlice.getBase(), (int) (compressedSlice.getAddress() - ARRAY_BYTE_BASE_OFFSET), compressedSlice.length());
                byte[] buffer = allocateOrGrowBuffer(compressedSlice.length() * EXPECTED_COMPRESSION_RATIO, false);
                int uncompressedLength = 0;
                while (true) {
                    uncompressedLength += inflater.inflate(buffer, uncompressedLength, buffer.length - uncompressedLength);
                    if (inflater.finished() || buffer.length >= maxBufferSize) {
                        break;
                    }
                    int oldBufferSize = buffer.length;
                    buffer = allocateOrGrowBuffer(buffer.length * 2, true);
                    if (buffer.length <= oldBufferSize) {
                        throw new IllegalStateException(String.format("Buffer failed to grow. Old size %d, current size %d", oldBufferSize, buffer.length));
                    }
                }

                if (!inflater.finished()) {
                    throw new OrcCorruptionException("Could not decompress all input (output buffer too small?)");
                }

                return Slices.wrappedBuffer(buffer, 0, uncompressedLength);
            }
            catch (DataFormatException e) {
                throw new OrcCorruptionException(e, "Invalid compressed stream");
            }
            finally {
                inflater.end();
            }
        }
    }
}
