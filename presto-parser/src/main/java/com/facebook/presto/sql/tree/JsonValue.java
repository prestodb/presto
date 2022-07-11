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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.tree.JsonValue.EmptyOrErrorBehavior.DEFAULT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JsonValue
        extends Expression
{
    private final JsonPathInvocation jsonPathInvocation;
    private final Optional<Expression> returnedType;
    private final EmptyOrErrorBehavior emptyBehavior;
    private final Optional<Expression> emptyDefault;
    private final EmptyOrErrorBehavior errorBehavior;
    private final Optional<Expression> errorDefault;

    public JsonValue(
            Optional<NodeLocation> location,
            JsonPathInvocation jsonPathInvocation,
            Optional<Expression> returnedType,
            EmptyOrErrorBehavior emptyBehavior,
            Optional<Expression> emptyDefault,
            EmptyOrErrorBehavior errorBehavior,
            Optional<Expression> errorDefault)
    {
        super(location);
        requireNonNull(jsonPathInvocation, "jsonPathInvocation is null");
        requireNonNull(returnedType, "returnedType is null");
        requireNonNull(emptyBehavior, "emptyBehavior is null");
        requireNonNull(emptyDefault, "emptyDefault is null");
        checkArgument(emptyBehavior == DEFAULT || !emptyDefault.isPresent(), "default value can be specified only for DEFAULT ... ON EMPTY option");
        checkArgument(emptyBehavior != DEFAULT || emptyDefault.isPresent(), "DEFAULT ... ON EMPTY option requires default value");
        requireNonNull(errorBehavior, "errorBehavior is null");
        requireNonNull(errorDefault, "errorDefault is null");
        checkArgument(errorBehavior == DEFAULT || !errorDefault.isPresent(), "default value can be specified only for DEFAULT ... ON ERROR option");
        checkArgument(errorBehavior != DEFAULT || errorDefault.isPresent(), "DEFAULT ... ON ERROR option requires default value");

        this.jsonPathInvocation = jsonPathInvocation;
        this.returnedType = returnedType;
        this.emptyBehavior = emptyBehavior;
        this.emptyDefault = emptyDefault;
        this.errorBehavior = errorBehavior;
        this.errorDefault = errorDefault;
    }

    public enum EmptyOrErrorBehavior
    {
        NULL, // default
        ERROR,
        DEFAULT
    }

    public JsonPathInvocation getJsonPathInvocation()
    {
        return jsonPathInvocation;
    }

    public Optional<Expression> getReturnedType()
    {
        return returnedType;
    }

    public EmptyOrErrorBehavior getEmptyBehavior()
    {
        return emptyBehavior;
    }

    public Optional<Expression> getEmptyDefault()
    {
        return emptyDefault;
    }

    public EmptyOrErrorBehavior getErrorBehavior()
    {
        return errorBehavior;
    }

    public Optional<Expression> getErrorDefault()
    {
        return errorDefault;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonValue(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        children.add(jsonPathInvocation);
        emptyDefault.ifPresent(children::add);
        errorDefault.ifPresent(children::add);
        return children.build();
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

        JsonValue that = (JsonValue) o;
        return Objects.equals(jsonPathInvocation, that.jsonPathInvocation) &&
                Objects.equals(returnedType, that.returnedType) &&
                emptyBehavior == that.emptyBehavior &&
                Objects.equals(emptyDefault, that.emptyDefault) &&
                errorBehavior == that.errorBehavior &&
                Objects.equals(errorDefault, that.errorDefault);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPathInvocation, returnedType, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }
}
