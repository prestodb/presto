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
package com.facebook.presto.common.predicate;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSortedRangeSet
{
    @Benchmark
    public SortedRangeSet benchmarkBuilder(Data data)
    {
        SortedRangeSet build = new SortedRangeSet.Builder(BIGINT)
                .addAll(data.ranges)
                .build();

        return build;
    }

    @Benchmark
    public List<Boolean> equalsSmall(Data data)
    {
        return benchmarkEquals(data.smallRanges);
    }

    @Benchmark
    public List<Boolean> equalsLarge(Data data)
    {
        return benchmarkEquals(data.largeRanges);
    }

    private List<Boolean> benchmarkEquals(List<SortedRangeSet> dataRanges)
    {
        List<Boolean> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).equals(dataRanges.get(index + 1)));
        }
        return result;
    }

    @Benchmark
    public List<SortedRangeSet> unionSmall(Data data)
    {
        return benchmarkUnion(data.smallRanges);
    }

    @Benchmark
    public List<SortedRangeSet> unionLarge(Data data)
    {
        return benchmarkUnion(data.largeRanges);
    }

    private List<SortedRangeSet> benchmarkUnion(List<SortedRangeSet> dataRanges)
    {
        List<SortedRangeSet> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).union(dataRanges.get(index + 1)));
        }
        return result;
    }

    @Benchmark
    public List<Boolean> overlapsSmall(Data data)
    {
        return benchmarkOverlaps(data.smallRanges);
    }

    @Benchmark
    public List<Boolean> overlapsLarge(Data data)
    {
        return benchmarkOverlaps(data.largeRanges);
    }

    private List<Boolean> benchmarkOverlaps(List<SortedRangeSet> dataRanges)
    {
        List<Boolean> result = new ArrayList<>(dataRanges.size() - 1);
        for (int index = 0; index < dataRanges.size() - 1; index++) {
            result.add(dataRanges.get(index).overlaps(dataRanges.get(index + 1)));
        }
        return result;
    }

    @Benchmark
    public long containsValueSmall(Data data)
    {
        return benchmarkContainsValue(data.smallRanges);
    }

    @Benchmark
    public long containsValueLarge(Data data)
    {
        return benchmarkContainsValue(data.largeRanges);
    }

    private long benchmarkContainsValue(List<SortedRangeSet> dataRanges)
    {
        int totalChecks = 5_000_000;
        long testedValuesTo = 10_000;

        long checksPerSet = totalChecks / dataRanges.size();
        long step = testedValuesTo / checksPerSet;
        long found = 0;
        for (SortedRangeSet dataRange : dataRanges) {
            for (long i = 0; i < testedValuesTo; i += step) {
                boolean contained = dataRange.containsValue(i);
                if (contained) {
                    found++;
                }
            }
        }
        return found;
    }

    @Benchmark
    public List<SortedRangeSet> complementSmall(Data data)
    {
        return benchmarkComplement(data.smallRanges);
    }

    @Benchmark
    public List<SortedRangeSet> complementLarge(Data data)
    {
        return benchmarkComplement(data.largeRanges);
    }

    private List<SortedRangeSet> benchmarkComplement(List<SortedRangeSet> dataRanges)
    {
        List<SortedRangeSet> result = new ArrayList<>(dataRanges.size());
        for (SortedRangeSet dataRange : dataRanges) {
            result.add(dataRange.complement());
        }
        return result;
    }

    @Benchmark
    public List<Integer> getOrderedRangesSmall(Data data)
    {
        return benchmarkGetOrderedRanges(data.smallRanges);
    }

    @Benchmark
    public List<Integer> getOrderedRangesLarge(Data data)
    {
        return benchmarkGetOrderedRanges(data.largeRanges);
    }

    private List<Integer> benchmarkGetOrderedRanges(List<SortedRangeSet> dataRanges)
    {
        List<Integer> result = new ArrayList<>(dataRanges.size());
        for (int index = 0; index < dataRanges.size(); index++) {
            int hash = 0;
            for (Range orderedRange : dataRanges.get(index).getRanges().getOrderedRanges()) {
                if (!orderedRange.isLowUnbounded()) {
                    hash = hash * 31 + orderedRange.getLowBoundedValue().hashCode();
                }
                if (!orderedRange.isHighUnbounded()) {
                    hash = hash * 31 + orderedRange.getHighBoundedValue().hashCode();
                }
            }
            result.add(hash);
        }
        return result;
    }

    @State(Scope.Thread)
    public static class Data
    {
        public List<Range> ranges;
        public List<SortedRangeSet> smallRanges;
        public List<SortedRangeSet> largeRanges;

        @Setup(Level.Iteration)
        public void init()
        {
            ranges = new ArrayList<>();

            int factor = 0;
            for (int i = 0; i < 10_000; i++) {
                long from = ThreadLocalRandom.current().nextLong(100) + factor * 100;
                long to = ThreadLocalRandom.current().nextLong(100) + (factor + 1) * 100;
                factor++;

                ranges.add(new Range(Marker.above(BIGINT, from), Marker.below(BIGINT, to)));
            }

            smallRanges = generateRangeSets(500_000, 2);
            largeRanges = generateRangeSets(5_000, 300);
        }

        private List<SortedRangeSet> generateRangeSets(int count, int size)
        {
            List<SortedRangeSet> sortedRangeSets = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                sortedRangeSets.add(generateRangeSet(size));
            }
            return sortedRangeSets;
        }

        private SortedRangeSet generateRangeSet(int size)
        {
            List<Range> selectedRanges = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                selectedRanges.add(ranges.get(ThreadLocalRandom.current().nextInt(ranges.size())));
            }
            return SortedRangeSet.copyOf(BIGINT, selectedRanges);
        }
    }

    @Test
    public void test()
    {
        Data data = new Data();
        data.init();

        benchmarkBuilder(data);

        equalsSmall(data);
        equalsLarge(data);

        unionSmall(data);
        unionLarge(data);

        overlapsSmall(data);
        overlapsLarge(data);

        containsValueSmall(data);
        containsValueLarge(data);

        complementSmall(data);
        complementLarge(data);

        getOrderedRangesSmall(data);
        getOrderedRangesLarge(data);
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
