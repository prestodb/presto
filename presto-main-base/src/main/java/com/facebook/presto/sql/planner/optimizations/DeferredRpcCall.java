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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DeferredRpcCall
{
    private final String functionName;
    private final Type outputType;
    private final List<RowExpression> arguments;
    private final int dispatchBatchSize;
    private final List<RowExpression> inputGroupingExpressions;

    private DeferredRpcCall(Builder builder)
    {
        this.functionName = builder.functionName;
        this.outputType = builder.outputType;
        this.arguments = ImmutableList.copyOf(builder.arguments);
        checkArgument(builder.dispatchBatchSize >= 0, "dispatch batch size must be non-negative");
        this.dispatchBatchSize = builder.dispatchBatchSize;
        this.inputGroupingExpressions = ImmutableList.copyOf(builder.inputGroupingExpressions);
    }

    public static Builder builder(String functionName, Type outputType)
    {
        return new Builder(functionName, outputType);
    }

    public String getFunctionName()
    {
        return functionName;
    }

    public Type getOutputType()
    {
        return outputType;
    }

    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    public int getDispatchBatchSize()
    {
        return dispatchBatchSize;
    }

    public List<RowExpression> getInputGroupingExpressions()
    {
        return inputGroupingExpressions;
    }

    public static final class Builder
    {
        private final String functionName;
        private final Type outputType;
        private List<RowExpression> arguments = ImmutableList.of();
        private int dispatchBatchSize;
        private List<RowExpression> inputGroupingExpressions = ImmutableList.of();

        private Builder(String functionName, Type outputType)
        {
            this.functionName = requireNonNull(functionName, "functionName is null");
            checkArgument(!functionName.isEmpty(), "functionName is empty");
            this.outputType = requireNonNull(outputType, "outputType is null");
        }

        public Builder setArguments(List<RowExpression> arguments)
        {
            this.arguments = requireNonNull(arguments, "arguments is null");
            return this;
        }

        public Builder setDispatchBatchSize(int dispatchBatchSize)
        {
            this.dispatchBatchSize = dispatchBatchSize;
            return this;
        }

        public Builder setInputGroupingExpressions(List<RowExpression> expressions)
        {
            this.inputGroupingExpressions = requireNonNull(expressions, "inputGroupingExpressions is null");
            return this;
        }

        public DeferredRpcCall build()
        {
            return new DeferredRpcCall(this);
        }
    }
}
