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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.spi.page.PageCompressor;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class AirliftCompressorAdapter
        implements PageCompressor
{
    private final Compressor compressor;

    public AirliftCompressorAdapter(Compressor compressor)
    {
        this.compressor = requireNonNull(compressor, "compressor is null");
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return compressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    public int compress(
            byte[] input,
            int inputOffset,
            int inputLength,
            byte[] output,
            int outputOffset,
            int maxOutputLength)
    {
        return compressor.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        compressor.compress(input, output);
    }
}
