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

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class HadoopCompressor
        implements RcFileCompressor
{
    private final CompressionCodec codec;

    public HadoopCompressor(CompressionCodec codec)
    {
        this.codec = requireNonNull(codec, "codec is null");
    }

    @Override
    public CompressedSliceOutput createCompressedSliceOutput(int minChunkSize, int maxChunkSize)
    {
        return new HadoopCompressedSliceOutputSupplier(codec, minChunkSize, maxChunkSize).get();
    }

    private static class HadoopCompressedSliceOutputSupplier
            implements Supplier<CompressedSliceOutput>
    {
        private final CompressionCodec codec;
        private final Compressor compressor;
        private final ChunkedSliceOutput bufferedOutput;

        public HadoopCompressedSliceOutputSupplier(CompressionCodec codec, int minChunkSize, int maxChunkSize)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.compressor = CodecPool.getCompressor(requireNonNull(codec, "codec is null"));
            this.bufferedOutput = new ChunkedSliceOutput(minChunkSize, maxChunkSize);
        }

        @Override
        public CompressedSliceOutput get()
        {
            try {
                compressor.reset();
                bufferedOutput.reset();
                CompressionOutputStream compressionStream = codec.createOutputStream(bufferedOutput, compressor);
                return new CompressedSliceOutput(compressionStream, bufferedOutput, this, () -> CodecPool.returnCompressor(compressor));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
