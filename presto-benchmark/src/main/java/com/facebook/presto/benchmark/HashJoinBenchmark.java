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

import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.NullOutputOperator.NullOutputOperatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class HashJoinBenchmark
        extends AbstractOperatorBenchmark
{
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators(new JoinProbeCompiler());
    private LookupSourceFactory lookupSourceFactory;

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
        if (lookupSourceFactory == null) {
            OperatorFactory ordersTableScan = createTableScanOperator(0, new PlanNodeId("test"), "orders", "orderkey", "totalprice");
            HashBuilderOperatorFactory hashBuilder = new HashBuilderOperatorFactory(
                    1,
                    new PlanNodeId("test"),
                    ordersTableScan.getTypes(),
                    ImmutableList.of(0, 1),
                    ImmutableMap.of(),
                    Ints.asList(0),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    1_500_000,
                    1,
                    new PagesIndex.TestingFactory());

            DriverContext driverContext = taskContext.addPipelineContext(0, false, false).addDriverContext();
            Driver driver = new DriverFactory(0, false, false, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty()).createDriver(driverContext);
            while (!hashBuilder.getLookupSourceFactory().createLookupSource().isDone()) {
                driver.process();
            }
            lookupSourceFactory = hashBuilder.getLookupSourceFactory();
        }

        OperatorFactory lineItemTableScan = createTableScanOperator(0, new PlanNodeId("test"), "lineitem", "orderkey", "quantity");

        OperatorFactory joinOperator = LOOKUP_JOIN_OPERATORS.innerJoin(1, new PlanNodeId("test"), lookupSourceFactory, lineItemTableScan.getTypes(), Ints.asList(0), Optional.empty(), Optional.empty());

        NullOutputOperatorFactory output = new NullOutputOperatorFactory(2, new PlanNodeId("test"), joinOperator.getTypes());

        DriverFactory driverFactory = new DriverFactory(1, true, true, ImmutableList.of(lineItemTableScan, joinOperator, output), OptionalInt.empty());
        DriverContext driverContext = taskContext.addPipelineContext(1, true, true).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }

    public static void main(String[] args)
    {
        new HashJoinBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
