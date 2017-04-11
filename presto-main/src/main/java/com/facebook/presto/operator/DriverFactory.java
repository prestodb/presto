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
package com.facebook.presto.operator;

import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DriverFactory
        implements Closeable
{
    private final int pipelineId;
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final List<OperatorFactory> operatorFactories;
    private final Optional<PlanNodeId> sourceId;
    private final OptionalInt driverInstances;
    private boolean closed;

    public DriverFactory(int pipelineId, boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances)
    {
        this.pipelineId = pipelineId;
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(requireNonNull(operatorFactories, "operatorFactories is null"));
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");
        this.driverInstances = requireNonNull(driverInstances, "driverInstances is null");

        List<PlanNodeId> sourceIds = operatorFactories.stream()
                .filter(SourceOperatorFactory.class::isInstance)
                .map(SourceOperatorFactory.class::cast)
                .map(SourceOperatorFactory::getSourceId)
                .collect(toImmutableList());
        checkArgument(sourceIds.size() <= 1, "Expected at most one source operator in driver facotry, but found %s", sourceIds);
        this.sourceId = sourceIds.isEmpty() ? Optional.empty() : Optional.of(sourceIds.get(0));
    }

    public int getPipelineId()
    {
        return pipelineId;
    }

    public boolean isInputDriver()
    {
        return inputDriver;
    }

    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    public Optional<PlanNodeId> getSourceId()
    {
        return sourceId;
    }

    public OptionalInt getDriverInstances()
    {
        return driverInstances;
    }

    public List<OperatorFactory> getOperatorFactories()
    {
        return operatorFactories;
    }

    public synchronized Driver createDriver(DriverContext driverContext)
    {
        checkState(!closed, "DriverFactory is already closed");
        requireNonNull(driverContext, "driverContext is null");
        ImmutableList.Builder<Operator> operators = ImmutableList.builder();
        for (OperatorFactory operatorFactory : operatorFactories) {
            Operator operator = operatorFactory.createOperator(driverContext);
            operators.add(operator);
        }
        return new Driver(driverContext, operators.build());
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            for (OperatorFactory operatorFactory : operatorFactories) {
                operatorFactory.close();
            }
        }
    }
}
