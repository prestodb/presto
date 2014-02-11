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

import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.LocalQueryRunner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class Top100Benchmark
        extends AbstractSimpleOperatorBenchmark
{
    public Top100Benchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "top100", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, "orders", "totalprice");
        TopNOperatorFactory topNOperator = new TopNOperatorFactory(
                1,
                100,
                ImmutableList.of(singleColumn(Type.DOUBLE, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortOrder.DESC_NULLS_LAST))),
                Optional.<Integer>absent(),
                false);
        return ImmutableList.of(tableScanOperator, topNOperator);
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new Top100Benchmark(createLocalQueryRunner(executor)).runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}
