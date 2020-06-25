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
package com.facebook.presto.sql;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DynamicFilters
{
    private DynamicFilters() {}

    public static Expression createDynamicFilterExpression(String id, SymbolReference input)
    {
        return new FunctionCall(QualifiedName.of(Function.NAME), ImmutableList.of(new StringLiteral(id), input));
    }

    public static ExtractResult extractDynamicFilters(RowExpression expression)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression);

        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        ImmutableList.Builder<Descriptor> dynamicConjuncts = ImmutableList.builder();

        for (RowExpression conjunct : conjuncts) {
            Optional<Descriptor> descriptor = getDescriptor(conjunct);
            if (descriptor.isPresent()) {
                dynamicConjuncts.add(descriptor.get());
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }

        return new ExtractResult(staticConjuncts.build(), dynamicConjuncts.build());
    }

    public static boolean isDynamicFilter(RowExpression expression)
    {
        return getDescriptor(expression).isPresent();
    }

    private static Optional<Descriptor> getDescriptor(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression callExpression = (CallExpression) expression;

        if (!callExpression.getDisplayName().equals(Function.NAME)) {
            return Optional.empty();
        }

        List<RowExpression> arguments = callExpression.getArguments();
        checkArgument(arguments.size() == 2, "invalid arguments count: %s", arguments.size());

        RowExpression firstArgument = arguments.get(0);
        checkArgument(firstArgument instanceof ConstantExpression, "firstArgument is expected to be an instance of ConstantExpression: %s", firstArgument.getClass().getSimpleName());
        String id = ((ConstantExpression) firstArgument).toString();
        return Optional.of(new Descriptor(id, arguments.get(1)));
    }

    public static class ExtractResult
    {
        private final List<RowExpression> staticConjuncts;
        private final List<Descriptor> dynamicConjuncts;

        public ExtractResult(List<RowExpression> staticConjuncts, List<Descriptor> dynamicConjuncts)
        {
            this.staticConjuncts = ImmutableList.copyOf(requireNonNull(staticConjuncts, "staticConjuncts is null"));
            this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        }

        public List<RowExpression> getStaticConjuncts()
        {
            return staticConjuncts;
        }

        public List<Descriptor> getDynamicConjuncts()
        {
            return dynamicConjuncts;
        }
    }

    public static final class Descriptor
    {
        private final String id;
        private final RowExpression input;

        public Descriptor(String id, RowExpression input)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
        }

        public String getId()
        {
            return id;
        }

        public RowExpression getInput()
        {
            return input;
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
            Descriptor that = (Descriptor) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(input, that.input);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .toString();
        }
    }

    @ScalarFunction(value = Function.NAME, deterministic = false, visibility = HIDDEN)
    public static final class Function
    {
        private Function() {}

        private static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Block input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") Slice input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") long input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") boolean input)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType(VARCHAR) Slice id, @SqlType("T") double input)
        {
            throw new UnsupportedOperationException();
        }
    }
}
