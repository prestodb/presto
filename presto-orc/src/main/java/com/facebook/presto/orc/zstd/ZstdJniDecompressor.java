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
package com.facebook.presto.orc.zstd;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.ByteBuffer;

import static java.lang.StrictMath.toIntExact;

public class ZstdJniDecompressor
        implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        long size = Zstd.decompressByteArray(output, 0, maxOutputLength, input, inputOffset, inputLength);
        if (Zstd.isError(size)) {
            String errorName = Zstd.getErrorName(size);
            throw new MalformedInputException(inputOffset, "Zstd JNI decompressor failed with " + errorName);
        }
        return toIntExact(size);
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output)
            throws MalformedInputException
    {
        if (input.isDirect() || output.isDirect() || !input.hasArray() || !output.hasArray()) {
            throw new IllegalArgumentException("Non-direct byte buffer backed by byte array required");
        }
        int inputOffset = input.arrayOffset() + input.position();
        int outputOffset = output.arrayOffset() + output.position();

        int written = decompress(input.array(), inputOffset, input.remaining(), output.array(), outputOffset, output.remaining());
        output.position(output.position() + written);
    }
}
