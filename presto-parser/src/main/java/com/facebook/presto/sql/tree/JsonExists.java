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

import static java.util.Objects.requireNonNull;

public class JsonExists
        extends Expression
{
    private final JsonPathInvocation jsonPathInvocation;
    private final ErrorBehavior errorBehavior;

    public JsonExists(
            Optional<NodeLocation> location,
            JsonPathInvocation jsonPathInvocation,
            ErrorBehavior errorBehavior)
    {
        super(location);
        requireNonNull(jsonPathInvocation, "jsonPathInvocation is null");
        requireNonNull(errorBehavior, "errorBehavior is null");

        this.jsonPathInvocation = jsonPathInvocation;
        this.errorBehavior = errorBehavior;
    }

    public enum ErrorBehavior
    {
        FALSE, // default
        TRUE,
        UNKNOWN,
        ERROR
    }

    public JsonPathInvocation getJsonPathInvocation()
    {
        return jsonPathInvocation;
    }

    public ErrorBehavior getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonExists(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(jsonPathInvocation);
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

        JsonExists that = (JsonExists) o;
        return Objects.equals(jsonPathInvocation, that.jsonPathInvocation) &&
                errorBehavior == that.errorBehavior;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPathInvocation, errorBehavior);
    }
}
