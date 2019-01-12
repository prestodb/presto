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
import io.prestosql.metadata.MetadataManager;
import io.prestosql.operator.Driver;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.DriverFactory;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.TaskContext;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.NullOutputOperator.NullOutputOperatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;

public abstract class AbstractSimpleOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

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

        operatorFactories.add(new NullOutputOperatorFactory(999, new PlanNodeId("test")));

        return new DriverFactory(0, true, true, operatorFactories, OptionalInt.empty(), UNGROUPED_EXECUTION);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        DriverFactory driverFactory = createDriverFactory();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }
}
