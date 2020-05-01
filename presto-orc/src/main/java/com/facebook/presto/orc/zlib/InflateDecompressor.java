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
package com.facebook.presto.orc.zlib;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class InflateDecompressor
        implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
            throws MalformedInputException
    {
        Inflater inflater = new Inflater(true);
        inflater.setInput(input, inputOffset, inputLength);
        int uncompressedLength = 0;
        try {
            uncompressedLength = inflater.inflate(output, outputOffset, maxOutputLength);
            if (!inflater.finished()) {
                throw new IllegalArgumentException("maxOutputLength is incorrect, there is more data to be decompressed");
            }
        }
        catch (DataFormatException e) {
            throw new MalformedInputException(inputOffset, e.getMessage());
        }
        finally {
            inflater.end();
        }
        return uncompressedLength;
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
