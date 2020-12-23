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
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.JoinBridgeManager;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupSourceProvider;
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
import java.util.concurrent.Future;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spiller.PartitioningSpillerFactory.unsupportedPartitioningSpillerFactory;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HashJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();
    private DriverFactory probeDriverFactory;

    public HashJoinBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_join", 4, 50);
    }

    /*
    select orderkey, quantity, totalprice
    from lineitem join orders using (orderkey)
     */

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        if (probeDriverFactory == null) {
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
                    SingleStreamSpillerFactory.unsupportedSingleStreamSpillerFactory(),
                    false);

            DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
            DriverFactory buildDriverFactory = new DriverFactory(0, false, false, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty(), UNGROUPED_EXECUTION, Optional.empty());

            List<Type> lineItemTypes = getColumnTypes("lineitem", "orderkey", "quantity");
            OperatorFactory lineItemTableScan = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "orderkey", "quantity");
            OperatorFactory joinOperator = LOOKUP_JOIN_OPERATORS.innerJoin(1, new PlanNodeId("test"), lookupSourceFactoryManager, lineItemTypes, Ints.asList(0), OptionalInt.empty(), Optional.empty(), OptionalInt.empty(), unsupportedPartitioningSpillerFactory());
            NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, new PlanNodeId("test"));
            this.probeDriverFactory = new DriverFactory(1, true, true, ImmutableList.of(lineItemTableScan, joinOperator, output), OptionalInt.empty(), UNGROUPED_EXECUTION, Optional.empty());

            Driver driver = buildDriverFactory.createDriver(driverContext);
            Future<LookupSourceProvider> lookupSourceProvider = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide()).createLookupSourceProvider();
            while (!lookupSourceProvider.isDone()) {
                driver.process();
            }
            getFutureValue(lookupSourceProvider).close();
        }

        DriverContext driverContext = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
        Driver driver = probeDriverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }

    public static void main(String[] args)
    {
        new HashJoinBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
