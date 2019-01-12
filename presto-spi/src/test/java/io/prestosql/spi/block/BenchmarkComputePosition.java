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
package io.prestosql.spi.block;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(NANOSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkComputePosition
{
    private int hashTableSize = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
    private long hashcode = ThreadLocalRandom.current().nextLong();

    // This is the baseline.
    @Benchmark
    public long computePositionWithFloorMod()
    {
        return Math.floorMod(hashcode, hashTableSize);
    }

    @Benchmark
    public long computePositionWithMod()
    {
        return (int) (hashcode & 0x7fff_ffff_ffff_ffffL) % hashTableSize;
    }

    // This reduction function requires the hashTableSize to be power of 2.
    @Benchmark
    public long computePositionWithMask()
    {
        return (int) hashcode & (hashTableSize - 1);
    }

    // This function reduces the 64 bit hashcode to [0, hashTableSize) uniformly if the hashcode has uniform distribution.
    // It first reduces the hashcode to 32 bit integer x then normalizes it to x / 2^32 * hashSize to reduce the range of x
    // from [0, 2^32) to [0, hashTableSize).
    @Benchmark
    public long computePositionWithBitShifting()
    {
        return (int) ((Integer.toUnsignedLong(Long.hashCode(hashcode)) * hashTableSize) >> 32);
    }

    // This function used division instead of bit shifting. JVM would compile it to bit shifting instructions thus it
    // has similar performance to computePositionWithBitShifting()
    @Benchmark
    public long computePositionWithDivision()
    {
        return (int) ((Integer.toUnsignedLong(Long.hashCode(hashcode)) * hashTableSize) / (1 << 32));
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkComputePosition.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
