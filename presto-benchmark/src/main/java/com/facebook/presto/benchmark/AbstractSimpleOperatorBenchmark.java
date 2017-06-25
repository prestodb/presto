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
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.NullOutputOperator.NullOutputOperatorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public abstract class AbstractSimpleOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler();

    protected AbstractSimpleOperatorBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        super(localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
    }

    protected abstract List<? extends OperatorFactory> createOperatorFactories();

    protected DriverFactory createDriverFactory()
    {
        List<OperatorFactory> operatorFactories = new ArrayList<>(createOperatorFactories());

        operatorFactories.add(new NullOutputOperatorFactory(999, new PlanNodeId("test"), Iterables.getLast(operatorFactories).getTypes()));

        return new DriverFactory(0, true, true, operatorFactories, OptionalInt.empty());
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        DriverFactory driverFactory = createDriverFactory();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }
}
