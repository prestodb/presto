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
package com.facebook.presto.spi.relation.column;

import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class LambdaDefinitionExpression
        extends ColumnExpression
{
    private final List<Type> argumentTypes;
    private final List<String> arguments;
    private final ColumnExpression body;

    public LambdaDefinitionExpression(List<Type> argumentTypes, List<String> arguments, ColumnExpression body)
    {
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.arguments = unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
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

    public ColumnExpression getBody()
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
        StringBuilder lambdaDefinition = new StringBuilder();
        lambdaDefinition.append("(").append(arguments.get(0));
        for (int i = 1; i < arguments.size(); i++) {
            lambdaDefinition.append(",").append(arguments.get(i));
        }
        lambdaDefinition.append(") -> ");
        lambdaDefinition.append(body);
        return lambdaDefinition.toString();
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
    public ColumnExpression replaceChildren(List<ColumnExpression> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Lambda defintion can only has one children");
        return new LambdaDefinitionExpression(argumentTypes, arguments, newChildren.get(0));
    }

    @Override
    public <R, C> R accept(ColumnExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }
}
