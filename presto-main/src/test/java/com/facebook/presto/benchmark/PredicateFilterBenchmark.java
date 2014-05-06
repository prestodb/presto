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
package com.facebook.presto.benchmark;

import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.util.LocalQueryRunner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class PredicateFilterBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public PredicateFilterBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "predicate_filter", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, "orders", "totalprice");
        FilterAndProjectOperatorFactory filterAndProjectOperator = new FilterAndProjectOperatorFactory(
                1,
                new DoubleFilter(50000.00),
                ImmutableList.of(singleColumn(DOUBLE, 0)));

        return ImmutableList.of(tableScanOperator, filterAndProjectOperator);
    }

    public static class DoubleFilter
            implements FilterFunction
    {
        private final double minValue;

        public DoubleFilter(double minValue)
        {
            this.minValue = minValue;
        }

        @Override
        public boolean filter(BlockCursor... cursors)
        {
            return cursors[0].getDouble() >= minValue;
        }

        @Override
        public boolean filter(RecordCursor cursor)
        {
            return cursor.getDouble(0) >= minValue;
        }
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new PredicateFilterBenchmark(createLocalQueryRunner(executor)).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
