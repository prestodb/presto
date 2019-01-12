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
package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.prestosql.operator.Driver;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.operator.JoinBridgeManager;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.PartitionedLookupSourceFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.ValuesOperator.ValuesOperatorFactory;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.NullOutputOperator.NullOutputOperatorFactory;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static java.util.Objects.requireNonNull;

public class HashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();

    public HashBuildBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_build", 4, 5);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        // hash build
        List<Type> ordersTypes = getColumnTypes("orders", "orderkey", "totalprice");
        OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                ordersTypes,
                ImmutableList.of(0, 1).stream()
                        .map(ordersTypes::get)
                        .collect(toImmutableList()),
                Ints.asList(0).stream()
                        .map(ordersTypes::get)
                        .collect(toImmutableList()),
                1,
                requireNonNull(ImmutableMap.of(), "layout is null"),
                false));
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(0, 1),
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                1_500_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory());
        DriverFactory hashBuildDriverFactory = new DriverFactory(0, true, true, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty(), UNGROUPED_EXECUTION);

        // empty join so build finishes
        ImmutableList.Builder<OperatorFactory> joinDriversBuilder = ImmutableList.builder();
        joinDriversBuilder.add(new ValuesOperatorFactory(0, new PlanNodeId("values"), ImmutableList.of()));
        OperatorFactory joinOperator = LOOKUP_JOIN_OPERATORS.innerJoin(
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                unsupportedPartitioningSpillerFactory());
        joinDriversBuilder.add(joinOperator);
        joinDriversBuilder.add(new NullOutputOperatorFactory(3, new PlanNodeId("test")));
        DriverFactory joinDriverFactory = new DriverFactory(1, true, true, joinDriversBuilder.build(), OptionalInt.empty(), UNGROUPED_EXECUTION);

        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(0, true, true, false).addDriverContext());
        hashBuildDriverFactory.noMoreDrivers();
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(1, true, true, false).addDriverContext());
        joinDriverFactory.noMoreDrivers();

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        new HashBuildBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
