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

import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static java.util.Collections.singleton;

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
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice");
        FilterAndProjectOperator.FilterAndProjectOperatorFactory filterAndProjectOperator = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                1,
                new PlanNodeId("test"),
                () -> new GenericPageProcessor(new DoubleFilter(50000.00), ImmutableList.of(singleColumn(DOUBLE, 0))),
                ImmutableList.of(DOUBLE));

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
        public boolean filter(int position, Block... blocks)
        {
            return DOUBLE.getDouble(blocks[0], position) >= minValue;
        }

        @Override
        public boolean filter(RecordCursor cursor)
        {
            return cursor.getDouble(0) >= minValue;
        }

        @Override
        public Set<Integer> getInputChannels()
        {
            return singleton(0);
        }
    }

    public static void main(String[] args)
    {
        new PredicateFilterBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
