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
package com.facebook.presto.orc;

import io.airlift.compress.snappy.SnappyDecompressor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

class OrcSnappyDecompressor
    implements OrcDecompressor
{
    private final int maxBufferSize;
    private final SnappyDecompressor decompressor = new SnappyDecompressor();

    public OrcSnappyDecompressor(int maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        int uncompressedLength = SnappyDecompressor.getUncompressedLength(input, offset);
        checkArgument(uncompressedLength <= maxBufferSize, "Snappy requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);

        // Snappy decompressor is more if there's at least a long's worth of extra space
        // in the output buffer
        byte[] buffer = output.initialize(uncompressedLength + SIZE_OF_LONG);
        return decompressor.decompress(input, offset, length, buffer, 0, buffer.length);
    }

    @Override
    public String toString()
    {
        return "snappy";
    }
}
