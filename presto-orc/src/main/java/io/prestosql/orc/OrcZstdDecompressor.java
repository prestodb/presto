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
package io.prestosql.orc;

import io.airlift.compress.MalformedInputException;
import io.airlift.compress.zstd.ZstdDecompressor;

import static java.lang.StrictMath.toIntExact;
import static java.util.Objects.requireNonNull;

class OrcZstdDecompressor
        implements OrcDecompressor
{
    private final OrcDataSourceId orcDataSourceId;
    private final int maxBufferSize;
    private final ZstdDecompressor decompressor = new ZstdDecompressor();

    public OrcZstdDecompressor(OrcDataSourceId orcDataSourceId, int maxBufferSize)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException
    {
        try {
            long uncompressedLength = ZstdDecompressor.getDecompressedSize(input, offset, length);
            if (uncompressedLength > maxBufferSize) {
                throw new OrcCorruptionException(orcDataSourceId, "Zstd requires buffer (%s) larger than max size (%s)", uncompressedLength, maxBufferSize);
            }

            byte[] buffer = output.initialize(toIntExact(uncompressedLength));
            return decompressor.decompress(input, offset, length, buffer, 0, buffer.length);
        }
        catch (MalformedInputException e) {
            throw new OrcCorruptionException(e, orcDataSourceId, "Invalid compressed stream");
        }
    }

    @Override
    public String toString()
    {
        return "zstd";
    }
}
