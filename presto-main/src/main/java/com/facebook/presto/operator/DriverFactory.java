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
import com.google.common.collect.ImmutableSet;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverFactory
        implements Closeable
{
    private final boolean inputDriver;
    private final boolean outputDriver;
    private final List<OperatorFactory> operatorFactories;
    private final Set<PlanNodeId> sourceIds;
    private final int driverInstances;
    private boolean closed;

    public DriverFactory(boolean inputDriver, boolean outputDriver, OperatorFactory firstOperatorFactory, OperatorFactory... otherOperatorFactories)
    {
        this(inputDriver,
                outputDriver,
                ImmutableList.<OperatorFactory>builder()
                        .add(requireNonNull(firstOperatorFactory, "firstOperatorFactory is null"))
                        .add(requireNonNull(otherOperatorFactories, "otherOperatorFactories is null"))
                        .build(),
                1);
    }

    public DriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories)
    {
        this(inputDriver, outputDriver, operatorFactories, 1);
    }

    public DriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, int driverInstances)
    {
        this.inputDriver = inputDriver;
        this.outputDriver = outputDriver;
        this.operatorFactories = ImmutableList.copyOf(requireNonNull(operatorFactories, "operatorFactories is null"));
        checkArgument(!operatorFactories.isEmpty(), "There must be at least one operator");
        this.driverInstances = driverInstances;

        ImmutableSet.Builder<PlanNodeId> sourceIds = ImmutableSet.builder();
        for (OperatorFactory operatorFactory : operatorFactories) {
            if (operatorFactory instanceof SourceOperatorFactory) {
                SourceOperatorFactory sourceOperatorFactory = (SourceOperatorFactory) operatorFactory;
                sourceIds.add(sourceOperatorFactory.getSourceId());
            }
        }
        this.sourceIds = sourceIds.build();
    }

    public boolean isInputDriver()
    {
        return inputDriver;
    }

    public boolean isOutputDriver()
    {
        return outputDriver;
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceIds;
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

    public int getDriverInstances()
    {
        return driverInstances;
    }
}
