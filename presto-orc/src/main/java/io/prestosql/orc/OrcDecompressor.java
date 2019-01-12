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

import io.airlift.units.DataSize;
import io.prestosql.orc.metadata.CompressionKind;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static java.lang.Math.toIntExact;

public interface OrcDecompressor
{
    int MAX_BUFFER_SIZE = toIntExact(new DataSize(4, MEGABYTE).toBytes());

    static Optional<OrcDecompressor> createOrcDecompressor(OrcDataSourceId orcDataSourceId, CompressionKind compression, int bufferSize)
            throws OrcCorruptionException
    {
        if ((compression != NONE) && ((bufferSize <= 0) || (bufferSize > MAX_BUFFER_SIZE))) {
            throw new OrcCorruptionException(orcDataSourceId, "Invalid compression block size: " + bufferSize);
        }
        switch (compression) {
            case NONE:
                return Optional.empty();
            case ZLIB:
                return Optional.of(new OrcZlibDecompressor(orcDataSourceId, bufferSize));
            case SNAPPY:
                return Optional.of(new OrcSnappyDecompressor(orcDataSourceId, bufferSize));
            case LZ4:
                return Optional.of(new OrcLz4Decompressor(orcDataSourceId, bufferSize));
            case ZSTD:
                return Optional.of(new OrcZstdDecompressor(orcDataSourceId, bufferSize));
            default:
                throw new OrcCorruptionException(orcDataSourceId, "Unknown compression type: " + compression);
        }
    }

    int decompress(byte[] input, int offset, int length, OutputBuffer output)
            throws OrcCorruptionException;

    interface OutputBuffer
    {
        byte[] initialize(int size);

        byte[] grow(int size);
    }
}
