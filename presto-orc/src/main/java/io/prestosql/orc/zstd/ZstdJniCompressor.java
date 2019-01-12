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
package io.prestosql.orc.zstd;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;

import static java.lang.Math.toIntExact;

public class ZstdJniCompressor
        implements Compressor
{
    private static final int COMPRESSION_LEVEL = 3; // default level

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
        throw new UnsupportedOperationException();
    }
}
