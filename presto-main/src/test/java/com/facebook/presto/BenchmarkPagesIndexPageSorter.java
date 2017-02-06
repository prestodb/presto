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
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkPagesIndexPageSorter
{
    @Benchmark
    public int runBenchmark(BenchmarkData data)
    {
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory());
        long[] addresses = pageSorter.sort(data.types, data.pages, data.sortChannels, nCopies(data.sortChannels.size(), ASC_NULLS_FIRST), 10_000);
        return addresses.length;
    }

    private static List<Page> createPages(int pageCount, int channelCount, Type type)
    {
        int positionCount = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / (channelCount * 8);

        List<Page> pages = new ArrayList<>(pageCount);
        for (int numPage = 0; numPage < pageCount; numPage++) {
            Block[] blocks = new Block[channelCount];
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                if (type.equals(BIGINT)) {
                    blocks[numChannel] = BlockAssertions.createLongSequenceBlock(0, positionCount);
                }
                else if (type.equals(VARCHAR)) {
                    blocks[numChannel] = BlockAssertions.createStringSequenceBlock(0, positionCount);
                }
                else if (type.equals(DOUBLE)) {
                    blocks[numChannel] = BlockAssertions.createDoubleSequenceBlock(0, positionCount);
                }
                else if (type.equals(BOOLEAN)) {
                    blocks[numChannel] = BlockAssertions.createBooleanSequenceBlock(0, positionCount);
                }
                else {
                    throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
            pages.add(new Page(blocks));
        }
        return pages;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({ "2", "3", "4", "5" })
        private int numSortChannels;

        @Param({ "BIGINT", "VARCHAR", "DOUBLE", "BOOLEAN" })
        private String sortChannelType;

        private List<Page> pages;
        private final int maxPages = 500;

        public List<Type> types;
        public List<Integer> sortChannels;

        @Setup
        public void setup()
        {
            int totalChannels = 20;
            Type type = getType();

            pages = createPages(maxPages, totalChannels, type);
            types = nCopies(totalChannels, type);

            sortChannels = new ArrayList<>();
            for (int i = 0; i < numSortChannels; i++) {
                sortChannels.add(i);
            }
        }

        private Type getType()
        {
            switch (sortChannelType) {
                case "BIGINT":
                    return BIGINT;
                case "VARCHAR":
                    return VARCHAR;
                case "DOUBLE":
                    return DOUBLE;
                case "BOOLEAN":
                    return BOOLEAN;
            }
            throw new IllegalArgumentException("Unsupported type: " + sortChannelType);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkPagesIndexPageSorter.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
