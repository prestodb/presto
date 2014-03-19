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
package com.facebook.presto.spi;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Level;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSortedRangeSet
{
    @GenerateMicroBenchmark
    public SortedRangeSet benchmarkBuilder(Data data)
    {
        SortedRangeSet build = new SortedRangeSet.Builder(Integer.class)
                .addAll(data.ranges)
                .build();

        return build;
    }

    @State(Scope.Thread)
    public static class Data
    {
        public List<Range> ranges;

        @Setup(Level.Iteration)
        public void init()
        {
            ranges = new ArrayList<>();

            int factor = 0;
            for (int i = 0; i < 10000; i++) {
                int from = ThreadLocalRandom.current().nextInt(100) + factor * 100;
                int to = ThreadLocalRandom.current().nextInt(100) + (factor + 1) * 100;
                factor++;

                ranges.add(new Range(new Marker(new SerializableNativeValue(Integer.class, from), Marker.Bound.ABOVE),
                        new Marker(new SerializableNativeValue(Integer.class, to), Marker.Bound.BELOW)));
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSortedRangeSet.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
