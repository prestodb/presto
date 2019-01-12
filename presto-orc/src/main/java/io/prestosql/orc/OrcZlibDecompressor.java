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

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static java.util.Objects.requireNonNull;

class OrcZlibDecompressor
        implements OrcDecompressor
{
    private static final int EXPECTED_COMPRESSION_RATIO = 5;

    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;

    public OrcZlibDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(input, offset, length);
            byte[] buffer = output.initialize(Math.min(length * EXPECTED_COMPRESSION_RATIO, maxBufferSize));

            int uncompressedLength = 0;
            while (true) {
                uncompressedLength += inflater.inflate(buffer, uncompressedLength, buffer.length - uncompressedLength);
                if (inflater.finished() || buffer.length >= maxBufferSize) {
                    break;
                }
                int oldBufferSize = buffer.length;
                buffer = output.grow(Math.min(buffer.length * 2, maxBufferSize));
                if (buffer.length <= oldBufferSize) {
                    throw new IllegalStateException(String.format("Buffer failed to grow. Old size %d, current size %d", oldBufferSize, buffer.length));
                }
            }

            if (!inflater.finished()) {
                throw new OrcCorruptionException(orcDataSourceId, "Could not decompress all input (output buffer too small?)");
            }

            return uncompressedLength;
        }
        catch (DataFormatException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
        finally {
            inflater.end();
        }
    }

    @Override
    public String toString()
    {
        return "zlib";
    }
}
