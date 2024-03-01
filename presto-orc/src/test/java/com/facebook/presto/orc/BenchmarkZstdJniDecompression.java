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

import com.facebook.presto.orc.zstd.ZstdJniCompressor;
import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.OptionalInt;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)

public class BenchmarkZstdJniDecompression
{
    private static final ZstdJniCompressor compressor = new ZstdJniCompressor(OptionalInt.empty());
    private static final List<Unit> list = generateWorkload();
    private static final int sourceLength = 256 * 1024;
    private static byte[] decompressedBytes = new byte[sourceLength];

    @Benchmark
    public void decompressJni()
            throws OrcCorruptionException
    {
        decompressList(createOrcDecompressor(true));
    }

    @Benchmark
    public void decompressJava()
            throws OrcCorruptionException
    {
        decompressList(createOrcDecompressor(false));
    }

    private void decompressList(OrcDecompressor decompressor)
            throws OrcCorruptionException
    {
        for (Unit unit : list) {
            int outputSize = decompressor.decompress(unit.compressedBytes, 0, unit.compressedLength, new OrcDecompressor.OutputBuffer()
            {
                @Override
                public byte[] initialize(int size)
                {
                    return decompressedBytes;
                }

                @Override
                public byte[] grow(int size)
                {
                    throw new RuntimeException();
                }
            });
            assertEquals(outputSize, unit.sourceLength);
        }
    }

    private static List<Unit> generateWorkload()
    {
        ImmutableList.Builder<Unit> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < 10; i++) {
            byte[] sourceBytes = getAlphaNumericString(sourceLength).getBytes();
            byte[] compressedBytes = new byte[sourceLength * 32];
            int size = compressor.compress(sourceBytes, 0, sourceBytes.length, compressedBytes, 0, compressedBytes.length);
            builder.add(new Unit(sourceBytes, sourceLength, compressedBytes, size));
        }
        return builder.build();
    }

    private OrcDecompressor createOrcDecompressor(boolean zstdJniDecompressionEnabled)
    {
        return new OrcZstdDecompressor(new OrcDataSourceId("orc"), sourceLength, zstdJniDecompressionEnabled);
    }

    private static String getAlphaNumericString(int length)
    {
        String alphaNumericString = "USINDIA";

        StringBuilder stringBuilder = new StringBuilder(length);

        for (int index = 0; index < length; index++) {
            int arrayIndex = (int) (alphaNumericString.length() * Math.random());

            stringBuilder.append(alphaNumericString.charAt(arrayIndex));
        }
        return stringBuilder.toString();
    }

    static class Unit
    {
        final byte[] sourceBytes;
        final int sourceLength;
        final byte[] compressedBytes;
        final int compressedLength;

        public Unit(byte[] sourceBytes, int sourceLength, byte[] compressedBytes, int compressedLength)
        {
            this.sourceBytes = sourceBytes;
            this.sourceLength = sourceLength;
            this.compressedBytes = compressedBytes;
            this.compressedLength = compressedLength;
        }
    }
}
