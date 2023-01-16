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

import com.facebook.presto.sql.tree.JsonPathParameter.JsonFormat;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class JsonQuery
        extends Expression
{
    private final JsonPathInvocation jsonPathInvocation;
    private final Optional<Expression> returnedType;
    private final Optional<JsonFormat> outputFormat;
    private final ArrayWrapperBehavior wrapperBehavior;
    private final Optional<QuotesBehavior> quotesBehavior;
    private final EmptyOrErrorBehavior emptyBehavior;
    private final EmptyOrErrorBehavior errorBehavior;

    public JsonQuery(
            Optional<NodeLocation> location,
            JsonPathInvocation jsonPathInvocation,
            Optional<Expression> returnedType,
            Optional<JsonFormat> outputFormat,
            ArrayWrapperBehavior wrapperBehavior,
            Optional<QuotesBehavior> quotesBehavior,
            EmptyOrErrorBehavior emptyBehavior,
            EmptyOrErrorBehavior errorBehavior)
    {
        super(location);
        requireNonNull(jsonPathInvocation, "jsonPathInvocation is null");
        requireNonNull(returnedType, "returnedType is null");
        requireNonNull(outputFormat, "outputFormat is null");
        requireNonNull(wrapperBehavior, "wrapperBehavior is null");
        requireNonNull(quotesBehavior, "quotesBehavior is null");
        requireNonNull(emptyBehavior, "emptyBehavior is null");
        requireNonNull(errorBehavior, "errorBehavior is null");

        this.jsonPathInvocation = jsonPathInvocation;
        this.returnedType = returnedType;
        this.outputFormat = outputFormat;
        this.wrapperBehavior = wrapperBehavior;
        this.quotesBehavior = quotesBehavior;
        this.emptyBehavior = emptyBehavior;
        this.errorBehavior = errorBehavior;
    }

    public enum EmptyOrErrorBehavior
    {
        NULL("NULL"), // default behavior for ON ERROR and ON EMPTY conditions
        ERROR("ERROR"),
        EMPTY_ARRAY("EMPTY ARRAY"),
        EMPTY_OBJECT("EMPTY OBJECT");

        private final String label;

        EmptyOrErrorBehavior(String label)
        {
            this.label = label;
        }

        @Override
        public String toString()
        {
            return label;
        }
    }

    public enum ArrayWrapperBehavior
    {
        WITHOUT, // default
        CONDITIONAL,
        UNCONDITIONAL
    }

    public enum QuotesBehavior
    {
        KEEP, // default
        OMIT
    }

    public JsonPathInvocation getJsonPathInvocation()
    {
        return jsonPathInvocation;
    }

    public Optional<Expression> getReturnedType()
    {
        return returnedType;
    }

    public Optional<JsonFormat> getOutputFormat()
    {
        return outputFormat;
    }

    public ArrayWrapperBehavior getWrapperBehavior()
    {
        return wrapperBehavior;
    }

    public Optional<QuotesBehavior> getQuotesBehavior()
    {
        return quotesBehavior;
    }

    public EmptyOrErrorBehavior getEmptyBehavior()
    {
        return emptyBehavior;
    }

    public EmptyOrErrorBehavior getErrorBehavior()
    {
        return errorBehavior;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonQuery(this, context);
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

        JsonQuery that = (JsonQuery) o;
        return Objects.equals(jsonPathInvocation, that.jsonPathInvocation) &&
                Objects.equals(returnedType, that.returnedType) &&
                Objects.equals(outputFormat, that.outputFormat) &&
                wrapperBehavior == that.wrapperBehavior &&
                Objects.equals(quotesBehavior, that.quotesBehavior) &&
                emptyBehavior == that.emptyBehavior &&
                errorBehavior == that.errorBehavior;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jsonPathInvocation, returnedType, outputFormat, wrapperBehavior, quotesBehavior, emptyBehavior, errorBehavior);
    }
}
