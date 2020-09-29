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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HashAggregationBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    private final InternalAggregationFunction doubleSum;

    public HashAggregationBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_agg", 5, 25);

        FunctionAndTypeManager functionAndTypeManager = localQueryRunner.getMetadata().getFunctionAndTypeManager();
        doubleSum = functionAndTypeManager.getAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction("sum", fromTypes(DOUBLE)));
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        List<Type> tableTypes = getColumnTypes("orders", "orderstatus", "totalprice");
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderstatus", "totalprice");
        HashAggregationOperatorFactory aggregationOperator = new HashAggregationOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(tableTypes.get(0)),
                Ints.asList(0),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(doubleSum.bind(ImmutableList.of(1), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                JOIN_COMPILER,
                false);
        return ImmutableList.of(tableScanOperator, aggregationOperator);
    }

    public static void main(String[] args)
    {
        new HashAggregationBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
