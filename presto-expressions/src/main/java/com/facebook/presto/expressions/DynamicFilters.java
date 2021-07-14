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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionTreeRewriter.rewriteWith;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
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

    public static RowExpression extractDynamicConjuncts(List<RowExpression> conjuncts, LogicalRowExpressions logicalRowExpressions)
    {
        ImmutableList.Builder<RowExpression> dynamicConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            Optional<DynamicFilterPlaceholder> placeholder = getPlaceholder(conjunct);
            if (placeholder.isPresent()) {
                dynamicConjuncts.add(conjunct);
            }
        }

        return logicalRowExpressions.combineConjuncts(dynamicConjuncts.build());
    }

    public static RowExpression extractStaticConjuncts(List<RowExpression> conjuncts, LogicalRowExpressions logicalRowExpressions)
    {
        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            Optional<DynamicFilterPlaceholder> placeholder = getPlaceholder(conjunct);
            if (!placeholder.isPresent()) {
                staticConjuncts.add(conjunct);
            }
        }

        return logicalRowExpressions.combineConjuncts(staticConjuncts.build());
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
        List<RowExpression> arguments = call.getArguments();

        if (!call.getDisplayName().equals(DynamicFilterPlaceholderFunction.NAME)) {
            return Optional.empty();
        }

        checkArgument(arguments.size() == 3, "invalid arguments count: %s", arguments.size());

        RowExpression probeSymbol = arguments.get(0);
        RowExpression operatorExpression = arguments.get(1);
        checkArgument(operatorExpression instanceof ConstantExpression);
        checkArgument(operatorExpression.getType() instanceof VarcharType);
        String operator = ((Slice) ((ConstantExpression) operatorExpression).getValue()).toStringUtf8();

        RowExpression idExpression = arguments.get(2);
        checkArgument(idExpression instanceof ConstantExpression);
        checkArgument(idExpression.getType() instanceof VarcharType);
        String id = ((Slice) ((ConstantExpression) idExpression).getValue()).toStringUtf8();

        OperatorType operatorType = OperatorType.valueOf(operator);
        if (operatorType.isComparisonOperator()) {
            return Optional.of(new DynamicFilterPlaceholder(id, probeSymbol, operatorType));
        }
        return Optional.empty();
    }

    public static RowExpression removeNestedDynamicFilters(RowExpression expression)
    {
        return rewriteWith(new RowExpressionRewriter<AtomicBoolean>()
        {
            @Override
            public RowExpression rewriteRowExpression(RowExpression node, AtomicBoolean context, RowExpressionTreeRewriter<AtomicBoolean> treeRewriter)
            {
                return node;
            }

            @Override
            public RowExpression rewriteSpecialForm(SpecialFormExpression node, AtomicBoolean modified, RowExpressionTreeRewriter<AtomicBoolean> treeRewriter)
            {
                if (!isConjunctiveDisjunctive(node.getForm())) {
                    return node;
                }

                checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");

                ImmutableList.Builder<RowExpression> expressionBuilder = ImmutableList.builder();
                for (RowExpression argument : node.getArguments()) {
                    expressionBuilder.add(rewriteWith(this, argument, modified));
                }
                List<RowExpression> arguments = expressionBuilder.build();

                expressionBuilder = ImmutableList.builder();
                if (isDynamicFilter(arguments.get(0))) {
                    expressionBuilder.add(TRUE_CONSTANT);
                    modified.set(true);
                }
                else {
                    expressionBuilder.add(arguments.get(0));
                }

                if (isDynamicFilter(arguments.get(1))) {
                    expressionBuilder.add(TRUE_CONSTANT);
                    modified.set(true);
                }
                else {
                    expressionBuilder.add(arguments.get(1));
                }

                if (!modified.get()) {
                    return node;
                }

                arguments = expressionBuilder.build();
                if (node.getForm().equals(AND)) {
                    if (arguments.get(0).equals(TRUE_CONSTANT) && arguments.get(1).equals(TRUE_CONSTANT)) {
                        return TRUE_CONSTANT;
                    }

                    if (arguments.get(0).equals(TRUE_CONSTANT)) {
                        return arguments.get(1);
                    }

                    if (arguments.get(1).equals(TRUE_CONSTANT)) {
                        return arguments.get(0);
                    }
                }

                if (node.getForm().equals(OR) && (arguments.get(0).equals(TRUE_CONSTANT) || arguments.get(1).equals(TRUE_CONSTANT))) {
                    return TRUE_CONSTANT;
                }

                return new SpecialFormExpression(node.getForm(), node.getType(), arguments);
            }

            private boolean isConjunctiveDisjunctive(SpecialFormExpression.Form form)
            {
                return form == AND || form == OR;
            }
        }, expression, new AtomicBoolean(false));
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
        private final OperatorType operator;

        public DynamicFilterPlaceholder(String id, RowExpression input, OperatorType operator)
        {
            this.id = requireNonNull(id, "id is null");
            this.input = requireNonNull(input, "input is null");
            this.operator = requireNonNull(operator, "operator is null");
        }

        public DynamicFilterPlaceholder(String id, RowExpression input)
        {
            this(id, input, EQUAL);
        }

        public String getId()
        {
            return id;
        }

        public RowExpression getInput()
        {
            return input;
        }

        public OperatorType getOperator()
        {
            return operator;
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
                    Objects.equals(input, that.input) &&
                    Objects.equals(operator, that.operator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, input, operator);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("input", input)
                    .add("operator", operator)
                    .toString();
        }

        public Domain applyComparison(Domain domain)
        {
            if (domain.isNone() || domain.isAll()) {
                return domain;
            }
            Range span = domain.getValues().getRanges().getSpan();
            switch (operator) {
                case EQUAL:
                    return domain;
                case LESS_THAN: {
                    Range range = Range.lessThan(span.getType(), span.getHigh().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case LESS_THAN_OR_EQUAL: {
                    Range range = Range.lessThanOrEqual(span.getType(), span.getHigh().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN: {
                    Range range = Range.greaterThan(span.getType(), span.getLow().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                case GREATER_THAN_OR_EQUAL: {
                    Range range = Range.greaterThanOrEqual(span.getType(), span.getLow().getValue());
                    return Domain.create(ValueSet.ofRanges(range), false);
                }
                default:
                    throw new IllegalArgumentException("Unsupported dynamic filtering comparison operator: " + operator);
            }
        }
    }

    @ScalarFunction(value = DynamicFilterPlaceholderFunction.NAME, visibility = HIDDEN)
    public static final class DynamicFilterPlaceholderFunction
    {
        private DynamicFilterPlaceholderFunction() {}

        public static final String NAME = "$internal$dynamic_filter_function";

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") Block input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") Slice input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") long input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") boolean input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }

        @TypeParameter("T")
        @SqlType(BOOLEAN)
        public static boolean dynamicFilter(@SqlType("T") double input, @SqlType(VARCHAR) Slice operator, @SqlType(VARCHAR) Slice id)
        {
            throw new UnsupportedOperationException();
        }
    }
}
