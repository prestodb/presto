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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DateLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.Types;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.type.BigintType.BIGINT;
import static com.facebook.presto.type.BooleanType.BOOLEAN;
import static com.facebook.presto.type.DoubleType.DOUBLE;
import static com.facebook.presto.type.NullType.NULL;
import static com.facebook.presto.type.Types.isNumeric;
import static com.facebook.presto.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class ExpressionAnalyzer
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final boolean experimentalSyntaxEnabled;
    private final Session session;
    private final Map<QualifiedName, Integer> resolvedNames = new HashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> subExpressionTypes = new IdentityHashMap<>();
    private final Set<InPredicate> subqueryInPredicates = Collections.newSetFromMap(new IdentityHashMap<InPredicate, Boolean>());

    public ExpressionAnalyzer(Analysis analysis, Session session, Metadata metadata, boolean experimentalSyntaxEnabled)
    {
        this.analysis = checkNotNull(analysis, "analysis is null");
        this.session = checkNotNull(session, "session is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
    }

    public Map<QualifiedName, Integer> getResolvedNames()
    {
        return resolvedNames;
    }

    public IdentityHashMap<FunctionCall, FunctionInfo> getResolvedFunctions()
    {
        return resolvedFunctions;
    }

    public IdentityHashMap<Expression, Type> getSubExpressionTypes()
    {
        return subExpressionTypes;
    }

    public Set<InPredicate> getSubqueryInPredicates()
    {
        return subqueryInPredicates;
    }

    /**
     * @param tupleDescriptor the tuple descriptor to use to resolve QualifiedNames
     * @param context the namespace context of the surrounding query
     */
    public Type analyze(Expression expression, TupleDescriptor tupleDescriptor, AnalysisContext context)
    {
        Visitor visitor = new Visitor(tupleDescriptor);

        return expression.accept(visitor, context);
    }

    private class Visitor
            extends AstVisitor<Type, AnalysisContext>
    {
        private final TupleDescriptor tupleDescriptor;

        private Visitor(TupleDescriptor tupleDescriptor)
        {
            this.tupleDescriptor = checkNotNull(tupleDescriptor, "tupleDescriptor is null");
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, AnalysisContext context)
        {
            if (node.getType() != CurrentTime.Type.TIMESTAMP) {
                throw new SemanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getType().getName());
            }

            if (node.getPrecision() != null) {
                throw new SemanticException(NOT_SUPPORTED, node, "non-default precision not yet supported");
            }

            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, AnalysisContext context)
        {
            List<Integer> matches = tupleDescriptor.resolveFieldIndexes(node.getName());
            if (matches.isEmpty()) {
                throw new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", node.getName());
            }
            else if (matches.size() > 1) {
                throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", node.getName());
            }

            int fieldIndex = Iterables.getOnlyElement(matches);
            Field field = tupleDescriptor.getFields().get(fieldIndex);
            resolvedNames.put(node.getName(), fieldIndex);
            subExpressionTypes.put(node, field.getType());

            return field.getType();
        }

        @Override
        protected Type visitNotExpression(NotExpression node, AnalysisContext context)
        {
            Type value = process(node.getValue(), context);
            if (!isBooleanOrNull(value)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Value of logical NOT expression must evaluate to a BOOLEAN (actual: %s)", value);
            }

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, AnalysisContext context)
        {
            Type left = process(node.getLeft(), context);
            if (!isBooleanOrNull(left)) {
                throw new SemanticException(TYPE_MISMATCH, node.getLeft(), "Left side of logical expression must evaluate to a BOOLEAN (actual: %s)", left);
            }
            Type right = process(node.getRight(), context);
            if (!isBooleanOrNull(right)) {
                throw new SemanticException(TYPE_MISMATCH, node.getRight(), "Right side of logical expression must evaluate to a BOOLEAN (actual: %s)", right);
            }

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, AnalysisContext context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);

            if (!sameType(left, right)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with '%s': %s vs %s", node.getType().getValue(), left, right);
            }

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, AnalysisContext context)
        {
            process(node.getValue(), context);

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, AnalysisContext context)
        {
            process(node.getValue(), context);

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, AnalysisContext context)
        {
            Type first = process(node.getFirst(), context);
            Type second = process(node.getSecond(), context);

            if (!sameType(first, second)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with nullif: %s vs %s", first, second);
            }

            subExpressionTypes.put(node, first);
            return first;
        }

        @Override
        protected Type visitIfExpression(IfExpression node, AnalysisContext context)
        {
            Type condition = process(node.getCondition(), context);
            if (!isBooleanOrNull(condition)) {
                throw new SemanticException(TYPE_MISMATCH, node, "IF condition must be a boolean type: %s", condition);
            }

            Type first = process(node.getTrueValue(), context);
            if (!node.getFalseValue().isPresent()) {
                subExpressionTypes.put(node, first);
                return first;
            }

            Type second = process(node.getFalseValue().get(), context);
            if (!sameType(first, second)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Result types for IF must be the same: %s vs %s", first, second);
            }

            Type type = (!first.equals(NULL)) ? first : second;
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, AnalysisContext context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenOperand = process(whenClause.getOperand(), context);
                if (!isBooleanOrNull(whenOperand)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "WHEN clause must be a boolean type: %s", whenOperand);
                }
            }

            List<Type> types = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                types.add(process(whenClause.getResult(), context));
            }
            if (node.getDefaultValue() != null) {
                types.add(process(node.getDefaultValue(), context));
            }

            Type type = getSingleType(node, "clauses", types);
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, AnalysisContext context)
        {
            Type operand = process(node.getOperand(), context);
            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenOperand = process(whenClause.getOperand(), context);
                if (!sameType(operand, whenOperand)) {
                    throw new SemanticException(TYPE_MISMATCH, node, "CASE operand type does not match WHEN clause operand type: %s vs %s", operand, whenOperand);
                }
            }

            List<Type> types = new ArrayList<>();
            for (WhenClause whenClause : node.getWhenClauses()) {
                types.add(process(whenClause.getResult(), context));
            }
            if (node.getDefaultValue() != null) {
                types.add(process(node.getDefaultValue(), context));
            }

            Type type = getSingleType(node, "clauses", types);
            subExpressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, AnalysisContext context)
        {
            List<Type> operandTypes = new ArrayList<>();
            for (Expression expression : node.getOperands()) {
                operandTypes.add(process(expression, context));
            }

            Type type = getSingleType(node, "operands", operandTypes);
            subExpressionTypes.put(node, type);

            return type;
        }

        private Type getSingleType(Node node, String subTypeName, List<Type> subTypes)
        {
            subTypes = ImmutableList.copyOf(filter(subTypes, not(Predicates.<Type>equalTo(NULL))));
            Type firstOperand = Iterables.get(subTypes, 0, NULL);
            if (!Iterables.all(subTypes, sameTypePredicate(firstOperand))) {
                throw new SemanticException(TYPE_MISMATCH, node, "All %s must be the same type: %s", subTypeName, subTypes);
            }
            return firstOperand;
        }

        @Override
        protected Type visitNegativeExpression(NegativeExpression node, AnalysisContext context)
        {
            Type type = process(node.getValue(), context);
            if (!isNumericOrNull(type)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Value of negative operator must be numeric (actual: %s)", type);
            }

            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitArithmeticExpression(ArithmeticExpression node, AnalysisContext context)
        {
            Type left = process(node.getLeft(), context);
            Type right = process(node.getRight(), context);

            if (!isNumericOrNull(left)) {
                throw new SemanticException(TYPE_MISMATCH, node.getLeft(), "Left side of '%s' must be numeric (actual: %s)", node.getType().getValue(), left);
            }
            if (!isNumericOrNull(right)) {
                throw new SemanticException(TYPE_MISMATCH, node.getRight(), "Right side of '%s' must be numeric (actual: %s)", node.getType().getValue(), right);
            }

            if (left.equals(BIGINT) && right.equals(BIGINT)) {
                subExpressionTypes.put(node, BIGINT);
                return BIGINT;
            }

            subExpressionTypes.put(node, DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, AnalysisContext context)
        {
            Type value = process(node.getValue(), context);
            if (!isStringTypeOrNull(value)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Left side of LIKE expression must be a STRING (actual: %s)", value);
            }

            Type pattern = process(node.getPattern(), context);
            if (!isStringTypeOrNull(pattern)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Pattern for LIKE expression must be a STRING (actual: %s)", pattern);
            }

            if (node.getEscape() != null) {
                Type escape = process(node.getEscape(), context);
                if (!isStringTypeOrNull(escape)) {
                    throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Escape for LIKE expression must be a STRING (actual: %s)", escape);
                }
            }

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, VARCHAR);
            return VARCHAR;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitDateLiteral(DateLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, AnalysisContext context)
        {
            subExpressionTypes.put(node, NULL);
            return NULL;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, AnalysisContext context)
        {
            if (node.getWindow().isPresent()) {
                for (Expression expression : node.getWindow().get().getPartitionBy()) {
                    process(expression, context);
                }

                for (SortItem sortItem : node.getWindow().get().getOrderBy()) {
                    process(sortItem.getSortKey(), context);
                }
            }

            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                argumentTypes.add(process(expression, context));
            }

            FunctionInfo function = metadata.getFunction(node.getName(), argumentTypes.build(), context.isApproximate());

            resolvedFunctions.put(node, function);

            subExpressionTypes.put(node, function.getReturnType());

            return function.getReturnType();
        }

        @Override
        protected Type visitExtract(Extract node, AnalysisContext context)
        {
            Type type = process(node.getExpression(), context);
            if (!type.equals(BIGINT) && !type.equals(NULL)) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be LONG (actual %s)", type);
            }

            subExpressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, AnalysisContext context)
        {
            Type value = process(node.getValue(), context);
            Type min = process(node.getMin(), context);
            Type max = process(node.getMax(), context);

            if (isStringTypeOrNull(value) && isStringTypeOrNull(min) && isStringTypeOrNull(max)) {
                subExpressionTypes.put(node, BOOLEAN);
                return BOOLEAN;
            }
            if (isNumericOrNull(value) && isNumericOrNull(min) && isNumericOrNull(max)) {
                subExpressionTypes.put(node, BOOLEAN);
                return BOOLEAN;
            }
            throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Between value, min and max must be the same type (value: %s, min: %s, max: %s)", value, min, max);
        }

        @Override
        public Type visitCast(Cast node, AnalysisContext context)
        {
            process(node.getExpression(), context);

            Type type = Types.fromName(node.getType());
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast to type: " + node.getType());
            }
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitInPredicate(InPredicate node, AnalysisContext context)
        {
            Type valueType = process(node.getValue(), context);
            Type listType = process(node.getValueList(), context);

            if (node.getValueList() instanceof SubqueryExpression) {
                subqueryInPredicates.add(node);
            }

            if (valueType.equals(NULL)) {
                subExpressionTypes.put(node, NULL);
            }
            else if (!sameType(valueType, listType)) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable for 'IN': %s vs %s", valueType, listType);
            }

            subExpressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitInListExpression(InListExpression node, AnalysisContext context)
        {
            List<Type> types = new ArrayList<>();
            for (Expression value : node.getValues()) {
                types.add(process(value, context));
            }

            // make sure all types are the same
            Type type = getSingleType(node, "values", types);

            subExpressionTypes.put(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, AnalysisContext context)
        {
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session, experimentalSyntaxEnabled, Optional.<QueryExplainer>absent());
            TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

            // Scalar subqueries should only produce one column
            if (descriptor.getFields().size() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY, node, "Subquery expression must produce only one field. Found %s", descriptor.getFields().size());
            }

            Type type = Iterables.getOnlyElement(descriptor.getFields()).getType();

            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        public Type visitInputReference(InputReference node, AnalysisContext context)
        {
            Type type = tupleDescriptor.getFields().get(node.getInput().getChannel()).getType();
            subExpressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitExpression(Expression node, AnalysisContext context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }

    public static Predicate<Type> sameTypePredicate(final Type type)
    {
        return new Predicate<Type>()
        {
            public boolean apply(Type input)
            {
                return sameType(type, input);
            }
        };
    }

    public static boolean sameType(Type type1, Type type2)
    {
        return type1.equals(type2) || (isNumeric(type1) && isNumeric(type2)) || type1.equals(NULL) || type2.equals(NULL);
    }

    public static boolean isBooleanOrNull(Type type)
    {
        return type.equals(BOOLEAN) || type.equals(NULL);
    }

    public static boolean isNumericOrNull(Type type)
    {
        return isNumeric(type) || type.equals(NULL);
    }

    public static boolean isStringTypeOrNull(Type type)
    {
        return type.equals(VARCHAR) || type.equals(NULL);
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(Session session, Metadata metadata, Map<Symbol, Type> types, Expression expression)
    {
        return getExpressionTypes(session, metadata, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            final Map<Symbol, Type> types,
            Iterable<? extends Expression> expressions)
    {
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(new Analysis(), session, metadata, false);
        List<Field> fields = IterableTransformer.on(DependencyExtractor.extractUnique(expressions))
                .transform(new Function<Symbol, Field>()
                {
                    @Override
                    public Field apply(Symbol symbol)
                    {
                        Type type = types.get(symbol);
                        checkArgument(type != null, "No type for symbol %s", symbol);
                        return Field.newUnqualified(symbol.getName(), type);
                    }
                })
                .list();

        for (Expression expression : expressions) {
            expressionAnalyzer.analyze(expression, new TupleDescriptor(fields), new AnalysisContext());
        }
        return expressionAnalyzer.getSubExpressionTypes();
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(Session session, Metadata metadata, Map<Input, Type> types, Expression expression)
    {
        return getExpressionTypesFromInput(session, metadata, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(
            Session session,
            Metadata metadata,
            Map<Input, Type> types,
            Iterable<? extends Expression> expressions)
    {
        Field[] fields = new Field[types.size()];
        for (Entry<Input, Type> entry : types.entrySet()) {
            fields[entry.getKey().getChannel()] = Field.newUnqualified(Optional.<String>absent(), entry.getValue());
        }
        TupleDescriptor tupleDescriptor = new TupleDescriptor(fields);

        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(new Analysis(), session, metadata, false);
        for (Expression expression : expressions) {
            expressionAnalyzer.analyze(expression, tupleDescriptor, new AnalysisContext());
        }
        return expressionAnalyzer.getSubExpressionTypes();
    }
}
