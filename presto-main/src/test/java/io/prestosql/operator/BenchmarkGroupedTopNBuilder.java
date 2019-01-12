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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.block.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkGroupedTopNBuilder
{
    private static final int EXTENDED_PRICE = 0;
    private static final int DISCOUNT = 1;
    private static final int SHIP_DATE = 2;
    private static final int QUANTITY = 3;

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final List<Type> types = ImmutableList.of(DOUBLE, DOUBLE, VARCHAR, DOUBLE);
        private final PageWithPositionComparator comparator = new SimplePageWithPositionComparator(
                types,
                ImmutableList.of(0, 2),
                ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST));

        @Param({"1", "100", "10000", "1000000"})
        private String topN = "1";

        @Param({"1", "100", "10000", "1000000"})
        private String positions = "1";

        private Page page;
        private GroupedTopNBuilder topNBuilder;

        @Setup
        public void setup()
        {
            page = createInputPage(Integer.valueOf(positions), types);
            topNBuilder = new GroupedTopNBuilder(types, comparator, Integer.valueOf(topN), false, new NoChannelGroupByHash());
        }

        public GroupedTopNBuilder getTopNBuilder()
        {
            return topNBuilder;
        }

        public Page getPage()
        {
            return page;
        }
    }

    @Benchmark
    public List<Page> topN(BenchmarkData data)
    {
        data.getTopNBuilder().processPage(data.getPage()).process();
        return ImmutableList.copyOf(data.getTopNBuilder().buildResult());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkGroupedTopNBuilder().topN(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkGroupedTopNBuilder.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static Page createInputPage(int positions, List<Type> types)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        LineItemGenerator lineItemGenerator = new LineItemGenerator(1, 1, 1);
        Iterator<LineItem> iterator = lineItemGenerator.iterator();
        for (int i = 0; i < positions; i++) {
            pageBuilder.declarePosition();

            LineItem lineItem = iterator.next();
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(EXTENDED_PRICE), lineItem.getExtendedPrice());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(DISCOUNT), lineItem.getDiscount());
            DATE.writeLong(pageBuilder.getBlockBuilder(SHIP_DATE), lineItem.getShipDate());
            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(QUANTITY), lineItem.getQuantity());
        }
        return pageBuilder.build();
    }
}
