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
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
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
import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class HashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    public HashBuildBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "hash_build", 4, 5);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        // hash build
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
                1);
        DriverFactory hashBuildDriverFactory = new DriverFactory(true, true, ImmutableList.of(ordersTableScan, hashBuilder), OptionalInt.empty());
        Driver hashBuildDriver = hashBuildDriverFactory.createDriver(taskContext.addPipelineContext(true, true).addDriverContext());
        hashBuildDriverFactory.close();

        // empty join so build finishes
        ImmutableList.Builder<OperatorFactory> joinDriversBuilder = ImmutableList.builder();
        joinDriversBuilder.add(new ValuesOperatorFactory(0, new PlanNodeId("values"), ImmutableList.of(BIGINT), ImmutableList.of()));
        OperatorFactory joinOperator = LookupJoinOperators.innerJoin(2, new PlanNodeId("test"),
                hashBuilder.getLookupSourceFactory(),
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                Optional.empty(),
                Optional.empty());
        joinDriversBuilder.add(joinOperator);
        joinDriversBuilder.add(new NullOutputOperatorFactory(3, new PlanNodeId("test"), joinOperator.getTypes()));
        DriverFactory joinDriverFactory = new DriverFactory(true, true, joinDriversBuilder.build(), OptionalInt.empty());
        Driver joinDriver = joinDriverFactory.createDriver(taskContext.addPipelineContext(true, true).addDriverContext());
        joinDriverFactory.close();

        return ImmutableList.of(hashBuildDriver, joinDriver);
    }

    public static void main(String[] args)
    {
        new HashBuildBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
