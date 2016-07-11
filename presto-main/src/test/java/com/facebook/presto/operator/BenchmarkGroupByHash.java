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

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.XxHash64;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkGroupByHash
{
    private static final int POSITIONS = 10_000_000;
    private static final String GROUP_COUNT_STRING = "3000000";
    private static final int GROUP_COUNT = Integer.parseInt(GROUP_COUNT_STRING);
    private static final int EXPECTED_SIZE = 10_000;

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object groupByHashPreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false);
        data.getPages().forEach(groupByHash::getGroupIds);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object addPagePreCompute(BenchmarkData data)
    {
        GroupByHash groupByHash = new MultiChannelGroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), EXPECTED_SIZE, false);
        data.getPages().forEach(groupByHash::addPage);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public Object bigintGroupByHash(SingleChannelBenchmarkData data)
    {
        GroupByHash groupByHash = new BigintGroupByHash(0, data.getHashEnabled(), EXPECTED_SIZE);
        data.getPages().forEach(groupByHash::addPage);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int groupId = 0; groupId < groupByHash.getGroupCount(); groupId++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(groupId, pageBuilder, 0);
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pageBuilder.build();
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baseline(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        long[] table = new long[hashSize];
        Arrays.fill(table, -1);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = block.getLong(position, 0);

                int tablePosition = (int) (value & mask);
                while (table[tablePosition] != -1 && table[tablePosition] != value) {
                    tablePosition++;
                }
                if (table[tablePosition] == -1) {
                    table[tablePosition] = value;
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public long baselineBigArray(BaselinePagesData data)
    {
        int hashSize = arraySize(GROUP_COUNT, 0.9f);
        int mask = hashSize - 1;
        LongBigArray table = new LongBigArray(-1);
        table.ensureCapacity(hashSize);

        long groupIds = 0;
        for (Page page : data.getPages()) {
            Block block = page.getBlock(0);
            int positionCount = block.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                long value = BIGINT.getLong(block, position);

                int tablePosition = (int) XxHash64.hash(value) & mask;
                while (table.get(tablePosition) != -1 && table.get(tablePosition) != value) {
                    tablePosition++;
                }
                if (table.get(tablePosition) == -1) {
                    table.set(tablePosition, value);
                    groupIds++;
                }
            }
        }
        return groupIds;
    }

    private static List<Page> createPages(int positionCount, int groupCount, List<Type> types, boolean hashEnabled)
    {
        int channelCount = types.size();

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        if (hashEnabled) {
            types = ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT)));
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            int rand = ThreadLocalRandom.current().nextInt(groupCount);
            pageBuilder.declarePosition();
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
            }
            if (hashEnabled) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(channelCount), BigintOperators.hashCode(rand));
            }
            if (pageBuilder.isFull()) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BaselinePagesData
    {
        @Param("1")
        private int channelCount = 1;

        @Param("false")
        private boolean hashEnabled;

        @Param(GROUP_COUNT_STRING)
        private int groupCount;

        private List<Page> pages;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, groupCount, ImmutableList.of(BIGINT), hashEnabled);
        }

        public List<Page> getPages()
        {
            return pages;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class SingleChannelBenchmarkData
    {
        @Param("1")
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean hashEnabled = true;

        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        private List<Page> pages;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, GROUP_COUNT, ImmutableList.of(BIGINT), hashEnabled);
            types = Collections.<Type>nCopies(1, BIGINT);
            channels = new int[1];
            for (int i = 0; i < 1; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public boolean getHashEnabled()
        {
            return hashEnabled;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({ "1", "5", "10", "15", "20" })
        private int channelCount = 1;

        // todo add more group counts when JMH support programmatic ability to set OperationsPerInvocation
        @Param(GROUP_COUNT_STRING)
        private int groupCount = GROUP_COUNT;

        @Param({"true", "false"})
        private boolean hashEnabled;

        private List<Page> pages;
        private Optional<Integer> hashChannel;
        private List<Type> types;
        private int[] channels;

        @Setup
        public void setup()
        {
            pages = createPages(POSITIONS, groupCount, Collections.<Type>nCopies(channelCount, BIGINT), hashEnabled);
            hashChannel = hashEnabled ? Optional.of(channelCount) : Optional.empty();
            types = Collections.<Type>nCopies(channelCount, BIGINT);
            channels = new int[channelCount];
            for (int i = 0; i < channelCount; i++) {
                channels[i] = i;
            }
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public Optional<Integer> getHashChannel()
        {
            return hashChannel;
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public int[] getChannels()
        {
            return channels;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkGroupByHash().groupByHashPreCompute(data);
        new BenchmarkGroupByHash().addPagePreCompute(data);

        SingleChannelBenchmarkData singleChannelBenchmarkData = new SingleChannelBenchmarkData();
        singleChannelBenchmarkData.setup();
        new BenchmarkGroupByHash().bigintGroupByHash(singleChannelBenchmarkData);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGroupByHash.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
