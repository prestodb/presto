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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkGroupByHash
{
    @Benchmark
    public int runBenchmark(BenchmarkData data)
    {
        GroupByHash groupByHash = new GroupByHash(data.getTypes(), data.getChannels(), data.getHashChannel(), 100);
        int groupCount = 0;
        for (Page page : data.getPages()) {
            GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
            groupCount += (int) groupIds.getGroupCount();
        }
        return groupCount;
    }

    private static List<Page> createPages(int pageCount, int channelCount, int groupCount, List<Type> types)
    {
        int positionCount = BlockBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / (channelCount * 8);
        List<Page> pages = new ArrayList<>(pageCount);
        PageBuilder pageBuilder = new PageBuilder(types);
        for (int numPage = 0; numPage < pageCount; numPage++) {
            pageBuilder.reset();
            for (int i = 0; i < positionCount; i++) {
                int rand = ThreadLocalRandom.current().nextInt() % groupCount;
                for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(numChannel), rand);
                }
            }
            pages.add(pageBuilder.build());
        }
        return pages;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({ "2", "5", "10", "15", "20" })
        private int channelCount;

        @Param({ "2", "10", "100", "1000", "10000" })
        private int groupCount;

        @Param({ "true", "false" })
        private boolean hashEnabled;

        private final int maxPages = 20;
        private List<Page> pages;
        public Optional<Integer> hashChannel;
        public List<Type> types;
        public int[] channels;

        @Setup
        public void setup()
        {
            pages = createPages(maxPages, channelCount, groupCount, Collections.<Type>nCopies(channelCount, BIGINT));
            hashChannel = hashEnabled ? Optional.of(channelCount - 1) : Optional.empty();
            types = Collections.<Type>nCopies(channelCount - 1, BIGINT);
            channels = new int[channelCount - 1];
            for (int i = 0; i < channelCount - 1; i++) {
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
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGroupByHash.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
