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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkGroupedTopNBuilder
{
    private static final int HASH_GROUP = 0;
    private static final int EXTENDED_PRICE = 1;
    private static final int DISCOUNT = 2;
    private static final int SHIP_DATE = 3;
    private static final int QUANTITY = 4;

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final List<Type> types = ImmutableList.of(BIGINT, DOUBLE, DOUBLE, VARCHAR, DOUBLE);
        private final PageWithPositionComparator comparator = new SimplePageWithPositionComparator(
                types,
                ImmutableList.of(EXTENDED_PRICE, SHIP_DATE),
                ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST));

        private int seed = 42;

        @Param({"1", "100", "10000", "1000000"})
        private int topN = 1;

        @Param({"1", "100", "10000", "1000000"})
        private int positions = 1;

        @Param("1000")
        private int positionsPerPage = 1000;

        @Param({"1", "10", "1000"})
        private int groupCount = 10;

        private List<Page> page;
        private GroupedTopNBuilder topNBuilder;

        @Setup
        public void setup()
        {
            page = createInputPages(positions, types, positionsPerPage, groupCount, seed);
            GroupByHash groupByHash;
            if (groupCount > 1) {
                groupByHash = new BigintGroupByHash(HASH_GROUP, true, groupCount, UpdateMemory.NOOP);
            }
            else {
                groupByHash = new NoChannelGroupByHash();
            }
            topNBuilder = new GroupedTopNBuilder(types, comparator, topN, false, groupByHash);
        }

        public GroupedTopNBuilder getTopNBuilder()
        {
            return topNBuilder;
        }

        public List<Page> getPages()
        {
            return page;
        }
    }

    @Benchmark
    public void topN(BenchmarkData data, Blackhole blackhole)
    {
        GroupedTopNBuilder topNBuilder = data.getTopNBuilder();
        for (Page page : data.getPages()) {
            Work<?> work = topNBuilder.processPage(page);
            boolean finished;
            do {
                finished = work.process();
            } while (!finished);
        }

        Iterator<Page> results = topNBuilder.buildResult();
        while (results.hasNext()) {
            blackhole.consume(results.next());
        }
    }

    public List<Page> topNToList(BenchmarkData data)
    {
        GroupedTopNBuilder topNBuilder = data.getTopNBuilder();
        for (Page page : data.getPages()) {
            Work<?> work = topNBuilder.processPage(page);
            boolean finished;
            do {
                finished = work.process();
            } while (!finished);
        }
        return ImmutableList.copyOf(topNBuilder.buildResult());
    }

    @Test
    public void testTopNBenchmark()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        assertFalse(topNToList(data).isEmpty());
    }

    public static void main(String[] args)
            throws RunnerException, CommandLineOptionException
    {
        Options options = new OptionsBuilder()
                .parent(new CommandLineOptions(args))
                .include(".*" + BenchmarkGroupedTopNBuilder.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static List<Page> createInputPages(int positions, List<Type> types, int positionsPerPage, int groupCount, int seed)
    {
        Random random = new Random(seed);
        List<Page> pages = new ArrayList<>();
        PageBuilder pageBuilder = new PageBuilder(types);
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < positions; i++) {
            pageBuilder.declarePosition();

            LineItem lineItem = iterator.next();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(HASH_GROUP), groupCount > 1 ? random.nextInt(groupCount) : 1);
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.getExtendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.getDiscount());
            DATE.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), lineItem.getShipDate());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.getQuantity());

            if (pageBuilder.getPositionCount() >= positionsPerPage) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }

        return pages;
    }
}
