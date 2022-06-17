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

import com.facebook.presto.orc.stream.BooleanOutputStream;
import com.facebook.presto.orc.stream.BooleanOutputStreamOld;
import com.facebook.presto.orc.stream.ByteOutputStream;
import com.facebook.presto.orc.stream.ByteOutputStreamOld;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkByteOutputStream
{
    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkByteOutputStream.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Benchmark
    public ByteOutputStreamOld oldBytes(BenchmarkData data)
    {
        ByteOutputStreamOld out = new ByteOutputStreamOld(getOrcOutputBuffer());
        for (byte[] bytes : data.bytesBytes) {
            for (int i = 0; i < bytes.length; i++) {
                out.writeByte(bytes[i]);
            }
        }
        out.close();
        return out;
    }

    @Benchmark
    public ByteOutputStream newBytes(BenchmarkData data)
    {
        ByteOutputStream out = new ByteOutputStream(getOrcOutputBuffer());
        for (byte[] bytes : data.bytesBytes) {
            for (int i = 0; i < bytes.length; i++) {
                out.writeByte(bytes[i]);
            }
        }
        out.close();
        return out;
    }

    @Benchmark
    public BooleanOutputStreamOld oldBoolean(BenchmarkData data)
    {
        BooleanOutputStreamOld out = new BooleanOutputStreamOld(getOrcOutputBuffer());
        List<boolean[]> booleans = data.booleans;
        for (int j = 0; j < booleans.size(); j++) {
            boolean[] values = booleans.get(j);
            byte[] counts = data.bytesBytes.get(j);
            for (int i = 0; i < values.length; i++) {
                out.writeBooleans(Math.abs(counts[i]), values[i]);
            }
        }
        return out;
    }

    @Benchmark
    public BooleanOutputStream newBoolean(BenchmarkData data)
    {
        BooleanOutputStream out = new BooleanOutputStream(getOrcOutputBuffer());
        List<boolean[]> booleans = data.booleans;
        for (int j = 0; j < booleans.size(); j++) {
            boolean[] values = booleans.get(j);
            byte[] counts = data.bytesBytes.get(j);
            for (int i = 0; i < values.length; i++) {
                out.writeBooleans(Math.abs(counts[i]), values[i]);
            }
        }
        return out;
    }

    private static OrcOutputBuffer getOrcOutputBuffer()
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder().setCompressionKind(NONE).build();
        return new OrcOutputBuffer(columnWriterOptions, Optional.empty());
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final Random random = new Random(0);

        private List<byte[]> bytesBytes;
        private List<boolean[]> booleans;

        @Param({"500000"})
        private int positions = 500000;

        @Param({"100"})
        private int blockCount = 100;

        @Setup
        public void setup()
                throws Exception
        {

            {
                ImmutableList.Builder<byte[]> blocks = ImmutableList.builderWithExpectedSize(blockCount);
                for (int i = 0; i < blockCount; i++) {
                    byte[] block = new byte[positions];
                    random.nextBytes(block);
                    blocks.add(block);
                }
                this.bytesBytes = blocks.build();
            }

            {
                ImmutableList.Builder<boolean[]> blocks = ImmutableList.builderWithExpectedSize(blockCount);
                for (int i = 0; i < blockCount; i++) {
                    boolean[] block = new boolean[positions];
                    for (int j = 0; j < positions; j++) {
                        block[j] = random.nextBoolean();
                    }
                    blocks.add(block);
                }
                this.booleans = blocks.build();
            }
        }
    }
}
