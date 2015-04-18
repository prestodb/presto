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
package com.facebook.presto.testing;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class NullOutputOperator
        implements Operator
{
    public static class NullOutputFactory
            implements OutputFactory
    {
        @Override
        public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceTypes)
        {
            return new NullOutputOperatorFactory(operatorId, sourceTypes);
        }
    }

    public static class NullOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> types;

        public NullOutputOperatorFactory(int operatorId, List<Type> types)
        {
            this.operatorId = operatorId;
            this.types = types;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, NullOutputOperator.class.getSimpleName());
            return new NullOutputOperator(operatorContext, types);
        }

        @Override
        public void close()
        {
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private boolean finished;

    public NullOutputOperator(OperatorContext operatorContext, List<Type> types)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
