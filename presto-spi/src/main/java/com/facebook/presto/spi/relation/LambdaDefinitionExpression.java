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

import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class LambdaDefinitionExpression
        extends RowExpression
{
    private final List<Type> argumentTypes;
    private final List<String> arguments;
    private final RowExpression body;

    @JsonCreator
    public LambdaDefinitionExpression(
            @JsonProperty("argumentTypes") List<Type> argumentTypes,
            @JsonProperty("arguments") List<String> arguments,
            @JsonProperty("body") RowExpression body)
    {
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.arguments = unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
        checkArgument(argumentTypes.size() == arguments.size(), "Number of argument types does not match number of arguments");
        this.body = requireNonNull(body, "body is null");
    }

    @JsonProperty
    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public List<String> getArguments()
    {
        return arguments;
    }

    @JsonProperty
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
        return "(" + String.join(",", arguments) + ") -> " + body;
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
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambda(this, context);
    }

    private static void checkArgument(boolean condition, String message, Object... messageArgs)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(message, messageArgs));
        }
    }
}
