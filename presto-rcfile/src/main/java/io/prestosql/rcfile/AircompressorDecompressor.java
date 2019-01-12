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
package io.prestosql.rcfile;

import io.airlift.slice.Slice;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AircompressorDecompressor
        implements RcFileDecompressor
{
    private final CompressionCodec codec;

    public AircompressorDecompressor(CompressionCodec codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public void decompress(Slice compressed, Slice uncompressed)
            throws RcFileCorruptionException
    {
        try (CompressionInputStream decompressorStream = codec.createInputStream(compressed.getInput())) {
            uncompressed.setBytes(0, decompressorStream, uncompressed.length());
        }
        catch (IndexOutOfBoundsException | IOException e) {
            throw new RcFileCorruptionException(e, "Compressed stream is truncated");
        }
    }

    @Override
    public void destroy()
    {
    }
}
