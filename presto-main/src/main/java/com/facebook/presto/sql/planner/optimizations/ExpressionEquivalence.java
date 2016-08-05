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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;

import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.relational.SqlToRowExpressionTranslator.translate;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;

public class ExpressionEquivalence
{
    private static final Ordering<RowExpression> ROW_EXPRESSION_ORDERING = Ordering.from(new RowExpressionComparator());
    private static final CanonicalizationVisitor CANONICALIZATION_VISITOR = new CanonicalizationVisitor();
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public ExpressionEquivalence(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public boolean areExpressionsEquivalent(Session session, Expression leftExpression, Expression rightExpression, Map<Symbol, Type> types)
    {
        Map<Symbol, Integer> symbolInput = new HashMap<>();
        Map<Integer, Type> inputTypes = new HashMap<>();
        int inputId = 0;
        for (Entry<Symbol, Type> entry : types.entrySet()) {
            symbolInput.put(entry.getKey(), inputId);
            inputTypes.put(inputId, entry.getValue());
            inputId++;
        }
        RowExpression leftRowExpression = toRowExpression(session, leftExpression, symbolInput, inputTypes);
        RowExpression rightRowExpression = toRowExpression(session, rightExpression, symbolInput, inputTypes);

        RowExpression canonicalizedLeft = leftRowExpression.accept(CANONICALIZATION_VISITOR, null);
        RowExpression canonicalizedRight = rightRowExpression.accept(CANONICALIZATION_VISITOR, null);

        return canonicalizedLeft.equals(canonicalizedRight);
    }

    private RowExpression toRowExpression(Session session, Expression expression, Map<Symbol, Integer> symbolInput, Map<Integer, Type> inputTypes)
    {
        // replace qualified names with input references since row expressions do not support these
        Expression expressionWithInputReferences = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(symbolInput), expression);

        // determine the type of every expression
        IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(
                session,
                metadata,
                sqlParser,
                inputTypes,
                expressionWithInputReferences);

        // convert to row expression
        return translate(expressionWithInputReferences, SCALAR, expressionTypes, metadata.getFunctionRegistry(), metadata.getTypeManager(), session, false);
    }

    private static class CanonicalizationVisitor
            implements RowExpressionVisitor<Void, RowExpression>
    {
        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            call = new CallExpression(
                    call.getSignature(),
                    call.getType(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));

            String callName = call.getSignature().getName();

            if (callName.equals("AND") || callName.equals("OR")) {
                // if we have nested calls (of the same type) flatten them
                List<RowExpression> flattenedArguments = flattenNestedCallArgs(call);

                // only consider distinct arguments
                Set<RowExpression> distinctArguments = ImmutableSet.copyOf(flattenedArguments);
                if (distinctArguments.size() == 1) {
                    return Iterables.getOnlyElement(distinctArguments);
                }

                // canonicalize the argument order (i.e., sort them)
                List<RowExpression> sortedArguments = ROW_EXPRESSION_ORDERING.sortedCopy(distinctArguments);

                return new CallExpression(
                        internalScalarFunction(
                                callName,
                                BOOLEAN.getTypeSignature(),
                                distinctArguments.stream()
                                        .map(RowExpression::getType)
                                        .map(Type::getTypeSignature)
                                        .collect(toImmutableList())),
                        BOOLEAN,
                        sortedArguments);
            }

            if (callName.equals(mangleOperatorName(EQUAL)) || callName.equals(mangleOperatorName(NOT_EQUAL)) || callName.equals("IS_DISTINCT_FROM")) {
                // sort arguments
                return new CallExpression(
                        call.getSignature(),
                        call.getType(),
                        ROW_EXPRESSION_ORDERING.sortedCopy(call.getArguments()));
            }

            if (callName.equals(mangleOperatorName(GREATER_THAN)) || callName.equals(mangleOperatorName(GREATER_THAN_OR_EQUAL))) {
                // convert greater than to less than
                return new CallExpression(
                        new Signature(
                                callName.equals(mangleOperatorName(GREATER_THAN)) ? mangleOperatorName(LESS_THAN) : mangleOperatorName(LESS_THAN_OR_EQUAL),
                                SCALAR,
                                call.getSignature().getTypeVariableConstraints(),
                                call.getSignature().getLongVariableConstraints(),
                                call.getSignature().getReturnType(),
                                swapPair(call.getSignature().getArgumentTypes()),
                                false),
                        call.getType(),
                        swapPair(call.getArguments()));
            }

            return call;
        }

        public static List<RowExpression> flattenNestedCallArgs(CallExpression call)
        {
            String callName = call.getSignature().getName();
            ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
            for (RowExpression argument : call.getArguments()) {
                if (argument instanceof CallExpression && callName.equals(((CallExpression) argument).getSignature().getName())) {
                    // same call type, so flatten the args
                    newArguments.addAll(flattenNestedCallArgs((CallExpression) argument));
                }
                else {
                    newArguments.add(argument);
                }
            }
            return newArguments.build();
        }

        @Override
        public RowExpression visitConstant(ConstantExpression constant, Void context)
        {
            return constant;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Void context)
        {
            return node;
        }
    }

    private static class RowExpressionComparator
            implements Comparator<RowExpression>
    {
        private final Comparator<Object> classComparator = Ordering.arbitrary();
        private final ListComparator<RowExpression> argumentComparator = new ListComparator<>(this);

        @Override
        public int compare(RowExpression left, RowExpression right)
        {
            int result = classComparator.compare(left.getClass(), right.getClass());
            if (result != 0) {
                return result;
            }

            if (left instanceof CallExpression) {
                CallExpression leftCall = (CallExpression) left;
                CallExpression rightCall = (CallExpression) right;
                return ComparisonChain.start()
                        .compare(leftCall.getSignature().toString(), rightCall.getSignature().toString())
                        .compare(leftCall.getArguments(), rightCall.getArguments(), argumentComparator)
                        .result();
            }

            if (left instanceof ConstantExpression) {
                ConstantExpression leftConstant = (ConstantExpression) left;
                ConstantExpression rightConstant = (ConstantExpression) right;

                result = leftConstant.getType().getTypeSignature().toString().compareTo(right.getType().getTypeSignature().toString());
                if (result != 0) {
                    return result;
                }

                Object leftValue = leftConstant.getValue();
                Object rightValue = rightConstant.getValue();

                Class<?> javaType = leftConstant.getType().getJavaType();
                if (javaType == boolean.class) {
                    return ((Boolean) leftValue).compareTo((Boolean) rightValue);
                }
                if (javaType == byte.class || javaType == short.class || javaType == int.class || javaType == long.class) {
                    return Long.compare(((Number) leftValue).longValue(), ((Number) rightValue).longValue());
                }
                if (javaType == float.class || javaType == double.class) {
                    return Double.compare(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue());
                }
                if (javaType == Slice.class) {
                    return ((Slice) leftValue).compareTo((Slice) rightValue);
                }

                // value is some random type (say regex), so we just randomly choose a greater value
                // todo: support all known type
                return -1;
            }

            if (left instanceof InputReferenceExpression) {
                return Integer.compare(((InputReferenceExpression) left).getField(), ((InputReferenceExpression) right).getField());
            }

            throw new IllegalArgumentException("Unsupported RowExpression type " + left.getClass().getSimpleName());
        }
    }

    private static class ListComparator<T>
            implements Comparator<List<T>>
    {
        private final Comparator<T> elementComparator;

        public ListComparator(Comparator<T> elementComparator)
        {
            this.elementComparator = requireNonNull(elementComparator, "elementComparator is null");
        }

        @Override
        public int compare(List<T> left, List<T> right)
        {
            int compareLength = min(left.size(), right.size());
            for (int i = 0; i < compareLength; i++) {
                int result = elementComparator.compare(left.get(i), right.get(i));
                if (result != 0) {
                    return result;
                }
            }
            return Integer.compare(left.size(), right.size());
        }
    }

    private static <T> List<T> swapPair(List<T> pair)
    {
        requireNonNull(pair, "pair is null");
        checkArgument(pair.size() == 2, "Expected pair to have two elements");
        return ImmutableList.of(pair.get(1), pair.get(0));
    }
}
