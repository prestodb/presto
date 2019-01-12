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
package io.prestosql.operator;

import com.google.common.util.concurrent.MoreExecutors;
import io.prestosql.execution.Lifespan;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.TestingSession;
import io.prestosql.testing.TestingTaskContext;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;

public class TestingOperatorContext
{
    public static OperatorContext create(ScheduledExecutorService scheduledExecutor)
    {
        Executor executor = MoreExecutors.directExecutor();

        TaskContext taskContext = TestingTaskContext.createTaskContext(
                executor,
                scheduledExecutor,
                TestingSession.testSessionBuilder().build());

        MemoryTrackingContext pipelineMemoryContext = new MemoryTrackingContext(newSimpleAggregatedMemoryContext(), newSimpleAggregatedMemoryContext(), newSimpleAggregatedMemoryContext());

        PipelineContext pipelineContext = new PipelineContext(
                1,
                taskContext,
                executor,
                scheduledExecutor,
                pipelineMemoryContext,
                false,
                false,
                false);

        DriverContext driverContext = new DriverContext(
                pipelineContext,
                executor,
                scheduledExecutor,
                pipelineMemoryContext,
                Lifespan.taskWide());

        OperatorContext operatorContext = driverContext.addOperatorContext(
                1,
                new PlanNodeId("test"),
                "operator type");

        return operatorContext;
    }

    private TestingOperatorContext() {}
}
