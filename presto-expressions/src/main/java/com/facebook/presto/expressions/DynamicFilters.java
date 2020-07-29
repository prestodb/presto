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
package com.facebook.presto.expressions;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
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

    public static DynamicFilterExtractResult extractDynamicFilters(RowExpression expression)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression);

        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        ImmutableList.Builder<DynamicFilterPlaceholder> dynamicConjuncts = ImmutableList.builder();

        for (RowExpression conjunct : conjuncts) {
            Optional<DynamicFilterPlaceholder> placeholder = getPlaceholder(conjunct);
            if (placeholder.isPresent()) {
                dynamicConjuncts.add(placeholder.get());
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }

        return new DynamicFilterExtractResult(staticConjuncts.build(), dynamicConjuncts.build());
    }

    public static boolean isDynamicFilter(RowExpression expression)
    {
        return getPlaceholder(expression).isPresent();
    }

    public static Optional<DynamicFilterPlaceholder> getPlaceholder(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) expression;

        if (!call.getDisplayName().equals(DynamicFilterPlaceholderFunction.NAME)) {
            return Optional.empty();
        }

        List<RowExpression> arguments = call.getArguments();
        checkArgument(arguments.size() == 2, "invalid arguments count: %s", arguments.size());

        RowExpression firstArgument = arguments.get(0);
        checkArgument(firstArgument instanceof ConstantExpression);
        checkArgument(firstArgument.getType() instanceof VarcharType);

        String id = ((Slice) ((ConstantExpression) firstArgument).getValue()).toStringUtf8();
        return Optional.of(new DynamicFilterPlaceholder(id, arguments.get(1)));
    }

    public static class DynamicFilterExtractResult
    {
        private final List<RowExpression> staticConjuncts;
        private final List<DynamicFilterPlaceholder> dynamicConjuncts;

        public DynamicFilterExtractResult(List<RowExpression> staticConjuncts, List<DynamicFilterPlaceholder> dynamicConjuncts)
        {
            this.staticConjuncts = ImmutableList.copyOf(requireNonNull(staticConjuncts, "staticConjuncts is null"));
            this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        }

        public List<RowExpression> getStaticConjuncts()
        {
            return staticConjuncts;
        }

        public List<DynamicFilterPlaceholder> getDynamicConjuncts()
        {
            return dynamicConjuncts;
        }
    }

    public static final class DynamicFilterPlaceholder
    {
        private final String id;
        private final RowExpression input;

        public DynamicFilterPlaceholder(String id, RowExpression input)
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
            DynamicFilterPlaceholder that = (DynamicFilterPlaceholder) o;
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

    @ScalarFunction(value = DynamicFilterPlaceholderFunction.NAME, visibility = HIDDEN)
    public static final class DynamicFilterPlaceholderFunction
    {
        private DynamicFilterPlaceholderFunction() {}

        public static final String NAME = "$internal$dynamic_filter_function";

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
