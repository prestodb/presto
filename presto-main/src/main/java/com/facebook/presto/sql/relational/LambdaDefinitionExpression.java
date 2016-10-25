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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.FunctionType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class LambdaDefinitionExpression
        extends RowExpression
{
    private final List<Type> argumentTypes;
    private final List<String> arguments;
    private final RowExpression body;

    public LambdaDefinitionExpression(List<Type> argumentTypes, List<String> arguments, RowExpression body)
    {
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        checkArgument(argumentTypes.size() == arguments.size(), "Number of argument types does not match number of arguments");
        this.body = requireNonNull(body, "body is null");
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public List<String> getArguments()
    {
        return arguments;
    }

    public RowExpression getBody()
    {
        return body;
    }

    @Override
    public Type getType()
    {
        return new FunctionType(argumentTypes, body.getType());
    }

    @Override
    public String toString()
    {
        return "(" + Joiner.on("").join(arguments) + ") -> " + body;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LambdaDefinitionExpression that = (LambdaDefinitionExpression) o;
        return Objects.equals(argumentTypes, that.argumentTypes) &&
                Objects.equals(arguments, that.arguments) &&
                Objects.equals(body, that.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(argumentTypes, arguments, body);
    }

    @Override
    public <C, R> R accept(RowExpressionVisitor<C, R> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }
}
