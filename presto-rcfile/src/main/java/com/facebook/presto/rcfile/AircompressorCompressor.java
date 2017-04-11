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
package com.facebook.presto.rcfile;

import com.google.common.base.Throwables;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class AircompressorCompressor
        implements RcFileCompressor
{
    private final CompressionCodec codec;

    public AircompressorCompressor(CompressionCodec codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public CompressedSliceOutput createCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new AircompressorCompressedSliceOutputSupplier(codec, minChunkSize, maxChunkSize).get();
    }

    private static class AircompressorCompressedSliceOutputSupplier
            implements Supplier<CompressedSliceOutput>
    {
        private final CompressionCodec codec;
        private final Compressor compressor;
        private final ChunkedSliceOutput compressedOutput;

        public AircompressorCompressedSliceOutputSupplier(CompressionCodec codec, int minChunkSize, int maxChunkSize)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.compressor = codec.createCompressor();
            this.compressedOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public CompressedSliceOutput get()
        {
            try {
                compressor.reset();
                compressedOutput.reset();
                CompressionOutputStream compressionStream = codec.createOutputStream(compressedOutput, compressor);
                return new CompressedSliceOutput(compressionStream, compressedOutput, this, () -> { });
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
