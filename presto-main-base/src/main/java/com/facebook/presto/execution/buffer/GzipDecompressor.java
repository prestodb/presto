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
package com.facebook.presto.execution.buffer;

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

public class GzipDecompressor
        implements Decompressor
{
    @Override
    public int decompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength) throws MalformedInputException
    {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(input, inputOffset, inputLength);
                GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream)) {
            int totalRead = 0;
            int bytesRead;
            while (totalRead < maxOutputLength) {
                bytesRead = gzipInputStream.read(output, outputOffset + totalRead, maxOutputLength - totalRead);
                if (bytesRead == -1) {
                    break;
                }
                totalRead += bytesRead;
            }
            if (totalRead >= maxOutputLength && gzipInputStream.read() != -1) {
                throw new IllegalArgumentException("maxOutputLength is incorrect, there is more data to be decompressed");
            }
            return totalRead;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void decompress(ByteBuffer input, ByteBuffer output) throws MalformedInputException
    {
        int inputOffset = input.arrayOffset() + input.position();
        int outputOffset = output.arrayOffset() + output.position();
        int written = decompress(input.array(), inputOffset, input.remaining(), output.array(), outputOffset, output.remaining());
        ((Buffer) output).position(output.position() + written);
    }
}
