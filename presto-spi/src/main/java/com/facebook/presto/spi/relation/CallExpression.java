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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class CallExpression
        extends RowExpression
{
    private final FunctionHandle functionHandle;
    private final Type returnType;
    private final List<RowExpression> arguments;

    @JsonCreator
    public CallExpression(
            @JsonProperty("functionHandle") FunctionHandle functionHandle,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("arguments") List<RowExpression> arguments)
    {
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(returnType, "returnType is null");

        this.functionHandle = functionHandle;
        this.returnType = returnType;
        this.arguments = unmodifiableList(new ArrayList<>(arguments));
    }

    @JsonProperty
    public FunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @Override
    @JsonProperty("returnType")
    public Type getType()
    {
        return returnType;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return functionHandle.getSignature().getName() + "(" + String.join(", ", arguments.stream().map(RowExpression::toString).collect(Collectors.toList())) + ")";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionHandle, arguments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CallExpression other = (CallExpression) obj;
        return Objects.equals(this.functionHandle, other.functionHandle) && Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
