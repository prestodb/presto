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

import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class FilterAndProjectOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PageProcessor processor;
    private final List<Type> types;
    private boolean closed;

    public FilterAndProjectOperatorFactory(int operatorId, PageProcessor processor, List<Type> types)
    {
        this.operatorId = operatorId;
        this.processor = processor;
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
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, FilterAndProjectOperator.class.getSimpleName());
        return new FilterAndProjectOperator(operatorContext, types, processor);
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
