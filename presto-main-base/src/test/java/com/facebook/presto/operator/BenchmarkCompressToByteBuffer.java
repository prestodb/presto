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
package com.facebook.presto.operator;

import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.ByteBuffer;
import java.util.Random;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCompressToByteBuffer
{
    @Benchmark
    public void compressToByteBuffer(BenchmarkData data)
    {
        data.byteBuffer.mark();
        data.COMPRESSOR.compress(data.slice.toByteBuffer(), data.byteBuffer);
        data.byteBuffer.reset();
    }

    @Benchmark
    public void compressToByteArray(BenchmarkData data)
    {
        data.COMPRESSOR.compress((byte[]) data.slice.getBase(), 0, data.slice.length(), data.bytes, 0, data.MAX_COMPRESSED_SIZE);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final Random RANDOM = new Random(0);
        private static final Compressor COMPRESSOR = new Lz4Compressor();
        private static final int UNCOMPRESSED_SIZE = 1_000_000;
        private static final int MAX_COMPRESSED_SIZE = COMPRESSOR.maxCompressedLength(UNCOMPRESSED_SIZE);

        private final byte[] byteValues = new byte[UNCOMPRESSED_SIZE];
        private final Slice slice = Slices.wrappedBuffer(byteValues);

        private final byte[] bytes = new byte[MAX_COMPRESSED_SIZE];
        private final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_COMPRESSED_SIZE);

        @Setup
        public void setup()
        {
            // Generate discontinuous runs of random values and 0's to avoid LZ4 enters uncompressible fast-path
            int runLength = UNCOMPRESSED_SIZE / 10;
            byte[] randomBytes = new byte[runLength];
            for (int i = 0; i < 10; i += 2) {
                RANDOM.nextBytes(randomBytes);
                System.arraycopy(randomBytes, 0, byteValues, i * runLength, runLength);
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkCompressToByteBuffer.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
