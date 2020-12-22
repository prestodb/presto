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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.JoinBridgeManager;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PartitionedLookupSourceFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.NullOutputOperator.NullOutputOperatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunnerHashEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HashBuildAndJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private final boolean hashEnabled;
    private final List<Type> ordersTableTypes = getColumnTypes("orders", "orderkey", "totalprice");
    private final OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
    private final List<Type> lineItemTableTypes = getColumnTypes("lineitem", "orderkey", "quantity");
    private final OperatorFactory lineItemTableScan = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "orderkey", "quantity");
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();

    public HashBuildAndJoinBenchmark(Session session, LocalQueryRunner localQueryRunner)
    {
        super(session, localQueryRunner, "hash_build_and_join_hash_enabled_" + isHashEnabled(session), 4, 5);
        this.hashEnabled = isHashEnabled(session);
    }

    private static boolean isHashEnabled(Session session)
    {
        return SystemSessionProperties.isOptimizeHashGenerationEnabled(session);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */
    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        ImmutableList.Builder<OperatorFactory> driversBuilder = ImmutableList.builder();
        driversBuilder.add(ordersTableScan);
        List<Type> sourceTypes = ordersTableTypes;
        OptionalInt hashChannel = OptionalInt.empty();
        if (hashEnabled) {
            driversBuilder.add(createHashProjectOperator(1, new PlanNodeId("test"), sourceTypes));
            sourceTypes = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BIGINT)
                    .build();
            hashChannel = OptionalInt.of(sourceTypes.size() - 1);
        }

        // hash build
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                sourceTypes,
                ImmutableList.of(0, 1).stream()
                        .map(sourceTypes::get)
                        .collect(toImmutableList()),
                Ints.asList(0).stream()
                        .map(sourceTypes::get)
                        .collect(toImmutableList()),
                1,
                requireNonNull(ImmutableMap.of(), "layout is null"),
                false));
        HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(0, 1),
                Ints.asList(0),
                hashChannel,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                1_500_000,
                new PagesIndex.TestingFactory(false),
                false,
                SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory(),
                false);
        driversBuilder.add(hashBuilder);
        DriverFactory hashBuildDriverFactory = new DriverFactory(0, true, false, driversBuilder.build(), OptionalInt.empty(), UNGROUPED_EXECUTION, Optional.empty());

        // join
        ImmutableList.Builder<OperatorFactory> joinDriversBuilder = ImmutableList.builder();
        joinDriversBuilder.add(lineItemTableScan);
        sourceTypes = lineItemTableTypes;
        hashChannel = OptionalInt.empty();
        if (hashEnabled) {
            joinDriversBuilder.add(createHashProjectOperator(1, new PlanNodeId("test"), sourceTypes));
            sourceTypes = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BIGINT)
                    .build();
            hashChannel = OptionalInt.of(sourceTypes.size() - 1);
        }

        OperatorFactory joinOperator = LOOKUP_JOIN_OPERATORS.innerJoin(
                2,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                sourceTypes,
                Ints.asList(0),
                hashChannel,
                Optional.empty(),
                OptionalInt.empty(),
                unsupportedPartitioningSpillerFactory());
        joinDriversBuilder.add(joinOperator);
        joinDriversBuilder.add(new NullOutputOperatorFactory(3, new PlanNodeId("test")));
        DriverFactory joinDriverFactory = new DriverFactory(1, true, true, joinDriversBuilder.build(), OptionalInt.empty(), UNGROUPED_EXECUTION, Optional.empty());

        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(0, true, false, false).addDriverContext());
        hashBuildDriverFactory.noMoreDrivers();
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(1, true, true, false).addDriverContext());
        joinDriverFactory.noMoreDrivers();

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        new HashBuildAndJoinBenchmark(testSessionBuilder().build(), createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        new HashBuildAndJoinBenchmark(testSessionBuilder().build(), createLocalQueryRunnerHashEnabled()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
