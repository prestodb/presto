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
import com.facebook.presto.metadata.OperatorInfo;
import com.facebook.presto.metadata.OperatorInfo.OperatorType;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

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
import static com.google.common.base.Preconditions.checkState;

public class ExpressionAnalyzer
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final boolean experimentalSyntaxEnabled;
    private final Session session;
    private final Map<QualifiedName, Integer> resolvedNames = new HashMap<>();
    private final IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, OperatorInfo> resolvedOperators = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> expressionTypes = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> expressionCoercions = new IdentityHashMap<>();
    private final Set<InPredicate> subqueryInPredicates = Collections.newSetFromMap(new IdentityHashMap<InPredicate, Boolean>());

    public ExpressionAnalyzer(Analysis analysis, Session session, Metadata metadata, boolean experimentalSyntaxEnabled)
    {
        this.analysis = checkNotNull(analysis, "analysis is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
        this.session = session;
    }

    public Map<QualifiedName, Integer> getResolvedNames()
    {
        return resolvedNames;
    }

    public IdentityHashMap<FunctionCall, FunctionInfo> getResolvedFunctions()
    {
        return resolvedFunctions;
    }

    public IdentityHashMap<Expression, OperatorInfo> getResolvedOperators()
    {
        return resolvedOperators;
    }

    public IdentityHashMap<Expression, Type> getExpressionTypes()
    {
        return expressionTypes;
    }

    public IdentityHashMap<Expression, Type> getExpressionCoercions()
    {
        return expressionCoercions;
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
        public Type process(Node node, @Nullable AnalysisContext context)
        {
            // don't double processs a node
            Type type = expressionTypes.get(node);
            if (type != null) {
                return type;
            }
            return super.process(node, context);
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

            expressionTypes.put(node, BIGINT);
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
            expressionTypes.put(node, field.getType());

            return field.getType();
        }

        @Override
        protected Type visitNotExpression(NotExpression node, AnalysisContext context)
        {
            checkType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, AnalysisContext context)
        {
            checkType(context, node.getLeft(), BOOLEAN, "Left side of logical expression");
            checkType(context, node.getRight(), BOOLEAN, "Right side of logical expression");

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, AnalysisContext context)
        {
            OperatorType operatorType;
            if (node.getType() == ComparisonExpression.Type.IS_DISTINCT_FROM) {
                operatorType = OperatorType.EQUAL;
            }
            else {
                operatorType = OperatorType.valueOf(node.getType().name());
            }
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, AnalysisContext context)
        {
            process(node.getValue(), context);

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, AnalysisContext context)
        {
            process(node.getValue(), context);

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, AnalysisContext context)
        {
            Type type = checkSameType(context, node, "Types are not comparable with NULLIF: %s vs %s", node.getFirst(), node.getSecond());

            // todo NULLIF should be type of first argument, but that is difficult in current AST based expressions tree
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitIfExpression(IfExpression node, AnalysisContext context)
        {
            checkType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = checkSameType(context, node, "Result types for IF must be the same: %s vs %s", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, AnalysisContext context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                checkType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
            }

            Type type = checkSameType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            expressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, AnalysisContext context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                checkSameType(context, node, "CASE operand type does not match WHEN clause operand type: %s vs %s", node.getOperand(), whenClause.getOperand());
            }

            Type type = checkSameType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            expressionTypes.put(node, type);

            return type;
        }

        private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses, Expression defaultValue)
        {
            List<Expression> resultExpressions = new ArrayList<>();
            for (WhenClause whenClause : whenClauses) {
                resultExpressions.add(whenClause.getResult());
            }
            if (defaultValue != null) {
                resultExpressions.add(defaultValue);
            }
            return resultExpressions;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, AnalysisContext context)
        {
            Type type = checkSameType(context, "All COALESCE operands must be the same type: %s", node.getOperands());

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitNegativeExpression(NegativeExpression node, AnalysisContext context)
        {
            return getOperator(context, node, OperatorType.NEGATION, node.getValue());
        }

        @Override
        protected Type visitArithmeticExpression(ArithmeticExpression node, AnalysisContext context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getType().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, AnalysisContext context)
        {
            checkType(context, node.getValue(), VARCHAR, "Left side of LIKE expression");
            checkType(context, node.getPattern(), VARCHAR, "Pattern for LIKE expression");
            if (node.getEscape() != null) {
                checkType(context, node.getEscape(), VARCHAR, "Escape for LIKE expression");
            }

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, VARCHAR);
            return VARCHAR;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitDateLiteral(DateLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, AnalysisContext context)
        {
            expressionTypes.put(node, NULL);
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

            FunctionInfo function = metadata.resolveFunction(node.getName(), argumentTypes.build(), context.isApproximate());
            for (int i = 0; i < node.getArguments().size(); i++) {
                Expression expression = node.getArguments().get(i);
                Type type = function.getArgumentTypes().get(i);
                checkType(context, expression, type, String.format("Function %s argument %d", function.getHandle(), i));
            }
            resolvedFunctions.put(node, function);

            expressionTypes.put(node, function.getReturnType());

            return function.getReturnType();
        }

        @Override
        protected Type visitExtract(Extract node, AnalysisContext context)
        {
            checkType(context, node.getExpression(), BIGINT, "Type of argument to extract");

            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, AnalysisContext context)
        {
            return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(), node.getMax());
        }

        @Override
        public Type visitCast(Cast node, AnalysisContext context)
        {
            Type type = Types.fromName(node.getType());
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast to type: " + node.getType());
            }

            Type value = process(node.getExpression(), context);
            if (value != NULL) {
                try {
                    OperatorInfo operator = metadata.getOperator(OperatorType.CAST, type, ImmutableList.of(value));
                    resolvedOperators.put(node, operator);
                }
                catch (IllegalArgumentException e) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitInPredicate(InPredicate node, AnalysisContext context)
        {
            Expression value = node.getValue();
            process(value, context);

            Expression valueList = node.getValueList();
            process(valueList, context);

            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;

                checkSameType(context,
                        "IN value and list items must be the same type: %s",
                        ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build());
            }
            else if (valueList instanceof SubqueryExpression) {
                subqueryInPredicates.add(node);
            }

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitInListExpression(InListExpression node, AnalysisContext context)
        {
            Type type = checkSameType(context, "All IN list values must be the same type: %s", node.getValues());

            expressionTypes.put(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, AnalysisContext context)
        {
            checkState(session != null, "Subquery should not be present while determining expression types");
            StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, session, experimentalSyntaxEnabled, Optional.<QueryExplainer>absent());
            TupleDescriptor descriptor = analyzer.process(node.getQuery(), context);

            // Scalar subqueries should only produce one column
            if (descriptor.getFields().size() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SCALAR_SUBQUERY, node, "Subquery expression must produce only one field. Found %s", descriptor.getFields().size());
            }

            Type type = Iterables.getOnlyElement(descriptor.getFields()).getType();

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        public Type visitInputReference(InputReference node, AnalysisContext context)
        {
            Type type = tupleDescriptor.getFields().get(node.getInput().getChannel()).getType();
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitExpression(Expression node, AnalysisContext context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Type getOperator(AnalysisContext context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            OperatorInfo operatorInfo;
            try {
                operatorInfo = metadata.resolveOperator(operatorType, argumentTypes.build());
            }
            catch (IllegalArgumentException e) {
                // todo not correct
                throw new SemanticException(TYPE_MISMATCH, node, e.getMessage());
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                Type type = operatorInfo.getArgumentTypes().get(i);
                checkType(context, expression, type, String.format("Operator %s argument %d", operatorInfo, i));
            }
            resolvedOperators.put(node, operatorInfo);

            expressionTypes.put(node, operatorInfo.getReturnType());

            return operatorInfo.getReturnType();
        }

        private Type checkType(AnalysisContext context, Expression expression, Type expectedType, String message)
        {
            Type type = process(expression, context);
            if (!type.equals(expectedType)) {
                if (!type.equals(NULL) && !(type.equals(BIGINT) && expectedType.equals(DOUBLE))) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, type);
                }
                expressionCoercions.put(expression, expectedType);
            }
            return type;
        }

        private Type checkSameType(AnalysisContext context, Node node, String message, Expression first, Expression second)
        {
            Type firstType = null;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = null;
            if (second != null) {
                secondType = process(second, context);
            }

            if (first == null) {
                return secondType;
            }
            if (second == null) {
                return firstType;
            }
            if (firstType.equals(secondType)) {
                return firstType;
            }

            // coerce types if possible
            if (firstType.equals(NULL) || (secondType.equals(DOUBLE) && firstType.equals(BIGINT))) {
                expressionCoercions.put(first, secondType);
                return secondType;
            }
            if (secondType.equals(NULL) || (secondType.equals(BIGINT) && firstType.equals(DOUBLE))) {
                expressionCoercions.put(second, firstType);
                return firstType;
            }
            throw new SemanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private Type checkSameType(AnalysisContext context, String message, List<Expression> expressions)
        {
            // determine super type
            Type superType = NULL;
            for (Expression expression : expressions) {
                Type type = process(expression, context);
                if (superType.equals(type) || type.equals(NULL)) {
                    continue;
                }

                if (superType.equals(NULL)) {
                    superType = type;
                }
                else if (isNumeric(superType) && isNumeric(type)) {
                    superType = DOUBLE;
                }
            }

            // verify all expressions can be coerced to the superType
            for (Expression expression : expressions) {
                Type type = process(expression, context);
                if (!type.equals(superType)) {
                    if (!type.equals(NULL) && !(type.equals(BIGINT) && superType.equals(DOUBLE))) {
                        throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                    }
                    expressionCoercions.put(expression, superType);
                }
            }

            return superType;
        }
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(Session session, Metadata metadata, Map<Symbol, Type> types, Expression expression)
    {
        return getExpressionTypes(session, metadata, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(Session session, Metadata metadata, Map<Symbol, Type> types, Iterable<? extends Expression> expressions)
    {
        return analyzeExpressionsWithSymbols(session, metadata, types, expressions).getExpressionTypes();
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(Session session, Metadata metadata, Map<Input, Type> types, Expression expression)
    {
        return getExpressionTypesFromInput(session, metadata, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(Session session, Metadata metadata, Map<Input, Type> types, Iterable<? extends Expression> expressions)
    {
        return analyzeExpressionsWithInputs(session, metadata, types, expressions).getExpressionTypes();
    }

    public static ExpressionAnalysis analyzeExpressionsWithSymbols(Session session, Metadata metadata, final Map<Symbol, Type> types, Iterable<? extends Expression> expressions)
    {
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

        return analyzeExpressions(session, metadata, new TupleDescriptor(fields), expressions);
    }

    public static ExpressionAnalysis analyzeExpressionsWithInputs(Session session, Metadata metadata, Map<Input, Type> types, Iterable<? extends Expression> expressions)
    {
        Field[] fields = new Field[types.size()];
        for (Entry<Input, Type> entry : types.entrySet()) {
            fields[entry.getKey().getChannel()] = Field.newUnqualified(Optional.<String>absent(), entry.getValue());
        }
        TupleDescriptor tupleDescriptor = new TupleDescriptor(fields);

        return analyzeExpressions(session, metadata, tupleDescriptor, expressions);
    }

    private static ExpressionAnalysis analyzeExpressions(Session session, Metadata metadata, TupleDescriptor tupleDescriptor, Iterable<? extends Expression> expressions)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(new Analysis(), session, metadata, false);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, tupleDescriptor, new AnalysisContext());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getResolvedFunctions(),
                analyzer.getSubqueryInPredicates());
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            Metadata metadata,
            TupleDescriptor tupleDescriptor,
            Analysis analysis,
            boolean approximateQueriesEnabled,
            AnalysisContext context,
            Expression expression)
    {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, session, metadata, approximateQueriesEnabled);
        analyzer.analyze(expression, tupleDescriptor, context);

        IdentityHashMap<Expression, Type> expressionTypes = analyzer.getExpressionTypes();
        IdentityHashMap<Expression, Type> expressionCoercions = analyzer.getExpressionCoercions();
        IdentityHashMap<FunctionCall, FunctionInfo> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions);
        analysis.addFunctionInfos(resolvedFunctions);

        for (Expression subExpression : expressionTypes.keySet()) {
            analysis.addResolvedNames(subExpression, analyzer.getResolvedNames());
        }

        Set<InPredicate> subqueryInPredicates = analyzer.getSubqueryInPredicates();

        return new ExpressionAnalysis(expressionTypes, expressionCoercions, resolvedFunctions, subqueryInPredicates);
    }
}
