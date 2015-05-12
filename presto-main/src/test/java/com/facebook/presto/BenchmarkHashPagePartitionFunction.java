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
package com.facebook.presto;

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.PageBuilderStatus;
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
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkHashPagePartitionFunction
{
    @Benchmark
    public List<Page> runBenchmark(BenchmarkData data)
    {
        PagePartitionFunction partitionFunction = new HashPagePartitionFunction(0, 8, data.getChannels(), data.getHashChannel(), data.getTypes());
        return partitionFunction.partition(data.getPages());
    }

    private static List<Page> createPages(int pageCount, int channelCount)
    {
        int positionCount = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / (channelCount * 8);
        List<Page> pages = new ArrayList<>(pageCount);
        for (int numPage = 0; numPage < pageCount; numPage++) {
            Block[] blocks = new Block[channelCount];
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                blocks[numChannel] = BlockAssertions.createLongSequenceBlock(0, positionCount);
            }
            pages.add(new Page(blocks));
        }
        return pages;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({ "2", "5", "10", "15", "20" })
        private int channelCount;

        @Param({ "true", "false" })
        private boolean hashEnabled;

        private final int maxPages = 20;
        private List<Page> pages;
        public Optional<Integer> hashChannel;
        public List<Type> types;
        public List<Integer> channels;

        @Setup
        public void setup()
        {
            pages = createPages(maxPages, channelCount);
            hashChannel = hashEnabled ? Optional.of(channelCount - 1) : Optional.empty();
            types = Collections.<Type>nCopies(channelCount, BIGINT);
            channels = new ArrayList<>(channelCount - 1);
            for (int i = 0; i < channelCount - 1; i++) {
                channels.add(i);
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

        public List<Integer> getChannels()
        {
            return channels;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkHashPagePartitionFunction.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
