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

import com.facebook.presto.orc.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.orc.metadata.CompressionKind;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.iq80.snappy.Snappy;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class SnappyCodecProvider
        implements CodecProvider
{
    private final int maxBufferSize;

    public SnappyCodecProvider(int maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public Codec get(AbstractAggregatedMemoryContext memoryContext)
    {
        return new SnappyCodec(maxBufferSize, memoryContext);
    }

    @Override
    public CompressionKind getCompressionKind()
    {
        return CompressionKind.SNAPPY;
    }

    private class SnappyCodec
            extends Codec
    {
        private final int maxBufferSize;

        public SnappyCodec(int maxBufferSize, AbstractAggregatedMemoryContext systemMemoryContext)
        {
            super(maxBufferSize, systemMemoryContext);
            this.maxBufferSize = maxBufferSize;
        }

        @Override
        public Slice decompress(Slice compressedSlice)
                throws IOException
        {
            byte[] inArray = (byte[]) compressedSlice.getBase();
            int inOffset = (int) (compressedSlice.getAddress() - ARRAY_BYTE_BASE_OFFSET);
            int inLength = compressedSlice.length();

            int uncompressedLength = Snappy.getUncompressedLength(inArray, inOffset);
            checkArgument(uncompressedLength <= maxBufferSize, "Snappy requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
            byte[] buffer = allocateOrGrowBuffer(uncompressedLength, false);

            uncompressedLength = Snappy.uncompress(inArray, inOffset, inLength, buffer, 0);
            return Slices.wrappedBuffer(buffer, 0, uncompressedLength);
        }
    }
}
