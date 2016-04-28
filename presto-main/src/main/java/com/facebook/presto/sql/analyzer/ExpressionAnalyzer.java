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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.DenyAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.DecimalParseResult;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StackableAstVisitor;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.slice.SliceUtf8;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.RowType.RowField;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.timeHasTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampHasTimeZone;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newIdentityHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private final FunctionRegistry functionRegistry;
    private final TypeManager typeManager;
    private final Function<Node, StatementAnalyzer> statementAnalyzerFactory;
    private final IdentityHashMap<FunctionCall, Signature> resolvedFunctions = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Integer> resolvedNames = new IdentityHashMap<>();
    private final IdentityHashMap<Expression, Type> expressionTypes = new IdentityHashMap<>();
    private final Set<SubqueryExpression> scalarSubqueries = newIdentityHashSet();
    private final IdentityHashMap<Expression, Type> expressionCoercions = new IdentityHashMap<>();
    private final Set<InPredicate> subqueryInPredicates = newIdentityHashSet();
    private final Session session;

    public ExpressionAnalyzer(FunctionRegistry functionRegistry, TypeManager typeManager, Function<Node, StatementAnalyzer> statementAnalyzerFactory, Session session)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.session = requireNonNull(session, "session is null");
    }

    public Map<Expression, Integer> getResolvedNames()
    {
        return resolvedNames;
    }

    public IdentityHashMap<FunctionCall, Signature> getResolvedFunctions()
    {
        return resolvedFunctions;
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

    public Set<Expression> getColumnReferences()
    {
        return ImmutableSet.copyOf(resolvedNames.keySet());
    }

    /**
     * @param tupleDescriptor the tuple descriptor to use to resolve QualifiedNames
     * @param context the namespace context of the surrounding query
     */
    public Type analyze(Expression expression, RelationType tupleDescriptor, AnalysisContext context)
    {
        Visitor visitor = new Visitor(tupleDescriptor);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    public Set<SubqueryExpression> getScalarSubqueries()
    {
        return scalarSubqueries;
    }

    private class Visitor
            extends StackableAstVisitor<Type, AnalysisContext>
    {
        private final RelationType tupleDescriptor;

        private Visitor(RelationType tupleDescriptor)
        {
            this.tupleDescriptor = requireNonNull(tupleDescriptor, "tupleDescriptor is null");
        }

        @SuppressWarnings("SuspiciousMethodCalls")
        @Override
        public Type process(Node node, @Nullable StackableAstVisitorContext<AnalysisContext> context)
        {
            // don't double process a node
            Type type = expressionTypes.get(node);
            if (type != null) {
                return type;
            }
            return super.process(node, context);
        }

        @Override
        protected Type visitRow(Row node, StackableAstVisitorContext<AnalysisContext> context)
        {
            List<Type> types = node.getItems().stream()
                    .map((child) -> process(child, context))
                    .collect(toImmutableList());

            Type type = new RowType(types, Optional.empty());
            expressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, StackableAstVisitorContext<AnalysisContext> context)
        {
            if (node.getPrecision() != null) {
                throw new SemanticException(NOT_SUPPORTED, node, "non-default precision not yet supported");
            }

            Type type;
            switch (node.getType()) {
                case DATE:
                    type = DATE;
                    break;
                case TIME:
                    type = TIME_WITH_TIME_ZONE;
                    break;
                case LOCALTIME:
                    type = TIME;
                    break;
                case TIMESTAMP:
                    type = TIMESTAMP_WITH_TIME_ZONE;
                    break;
                case LOCALTIMESTAMP:
                    type = TIMESTAMP;
                    break;
                default:
                    throw new SemanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getType().getName());
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitQualifiedNameReference(QualifiedNameReference node, StackableAstVisitorContext<AnalysisContext> context)
        {
            List<Field> matches = tupleDescriptor.resolveFields(node.getName());
            if (matches.isEmpty()) {
                throw createMissingAttributeException(node);
            }

            if (matches.size() > 1) {
                throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", node.getName());
            }

            Field field = Iterables.getOnlyElement(matches);
            int fieldIndex = tupleDescriptor.indexOf(field);
            resolvedNames.put(node, fieldIndex);
            expressionTypes.put(node, field.getType());
            return field.getType();
        }

        @Override
        protected Type visitDereferenceExpression(DereferenceExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                List<Field> matches = tupleDescriptor.resolveFields(qualifiedName);
                if (matches.size() > 1) {
                    throw new SemanticException(AMBIGUOUS_ATTRIBUTE, node, "Column '%s' is ambiguous", node);
                }

                if (matches.size() == 1) {
                    Field field = Iterables.getOnlyElement(matches);
                    int fieldIndex = tupleDescriptor.indexOf(field);
                    resolvedNames.put(node, fieldIndex);
                    expressionTypes.put(node, field.getType());
                    return field.getType();
                }

                if (!isColumnPrefix(qualifiedName, tupleDescriptor)) {
                    if (isReferenceToOuterRelation(context.getContext(), qualifiedName)) {
                        throw new SemanticException(NOT_SUPPORTED, node, "Correlated queries not yet supported. Invalid column reference: '%s'", qualifiedName);
                    }
                    throw createMissingAttributeException(node);
                }
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType)) {
                throw new SemanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            RowType rowType = (RowType) baseType;

            Type rowFieldType = null;
            for (RowField rowField : rowType.getFields()) {
                if (node.getFieldName().equalsIgnoreCase(rowField.getName().orElse(null))) {
                    rowFieldType = rowField.getType();
                    break;
                }
            }
            if (rowFieldType == null) {
                throw createMissingAttributeException(node);
            }

            expressionTypes.put(node, rowFieldType);
            return rowFieldType;
        }

        private boolean isColumnPrefix(QualifiedName qualifiedName, RelationType tupleDescriptor)
        {
            // Recursively check if its prefix is a column.
            while (qualifiedName.getPrefix().isPresent()) {
                qualifiedName = qualifiedName.getPrefix().get();
                List<Field> matches = tupleDescriptor.resolveFields(qualifiedName);
                if (!matches.isEmpty()) {
                    return true;
                }
            }
            return false;
        }

        private boolean isReferenceToOuterRelation(AnalysisContext context, QualifiedName qualifiedName)
        {
            AnalysisContext current = context;
            while (current != null) {
                RelationType type = current.getParentRelationType();
                if (!type.resolveFields(qualifiedName).isEmpty()) {
                    return true;
                }
                if (isColumnPrefix(qualifiedName, type)) {
                    return true;
                }
                current = current.getParent().orElse(null);
            }
            return false;
        }

        private SemanticException createMissingAttributeException(Expression node)
        {
            return new SemanticException(MISSING_ATTRIBUTE, node, "Column '%s' cannot be resolved", node);
        }

        @Override
        protected Type visitNotExpression(NotExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            coerceType(context, node.getLeft(), BOOLEAN, "Left side of logical expression");
            coerceType(context, node.getRight(), BOOLEAN, "Right side of logical expression");

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<AnalysisContext> context)
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
        protected Type visitIsNullPredicate(IsNullPredicate node, StackableAstVisitorContext<AnalysisContext> context)
        {
            process(node.getValue(), context);

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, StackableAstVisitorContext<AnalysisContext> context)
        {
            process(node.getValue(), context);

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type firstType = process(node.getFirst(), context);
            Type secondType = process(node.getSecond(), context);

            if (!getCommonSuperTypeSignature(firstType.getTypeSignature(), secondType.getTypeSignature()).isPresent()) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            expressionTypes.put(node, firstType);
            return firstType;
        }

        @Override
        protected Type visitIfExpression(IfExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same: %s vs %s", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
            }

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            expressionTypes.put(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                requireNonNull(whenClauseType, format("Expression types does not contain an entry for %s", whenClause));
                expressionTypes.put(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceToSingleType(context, whenClause, "CASE operand type does not match WHEN clause operand type: %s vs %s", node.getOperand(), whenClause.getOperand());
            }

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            expressionTypes.put(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                requireNonNull(whenClauseType, format("Expression types does not contain an entry for %s", whenClause));
                expressionTypes.put(whenClause, whenClauseType);
            }

            return type;
        }

        private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses, Optional<Expression> defaultValue)
        {
            List<Expression> resultExpressions = new ArrayList<>();
            for (WhenClause whenClause : whenClauses) {
                resultExpressions.add(whenClause.getResult());
            }
            defaultValue.ifPresent(resultExpressions::add);
            return resultExpressions;
        }

        @Override
        protected Type visitCoalesceExpression(CoalesceExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = coerceToSingleType(context, "All COALESCE operands must be the same type: %s", node.getOperands());

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            switch (node.getSign()) {
                case PLUS:
                    Type type = process(node.getValue(), context);

                    if (!type.equals(BIGINT) && !type.equals(DOUBLE) && !type.equals(INTEGER)) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw new SemanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    expressionTypes.put(node, type);
                    return type;
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getType().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type valueType = getVarcharType(node.getValue(), context);
            Type patternType = getVarcharType(node.getPattern(), context);
            coerceType(context, node.getValue(), valueType, "Left side of LIKE expression");
            coerceType(context, node.getPattern(), patternType, "Pattern for LIKE expression");
            if (node.getEscape() != null) {
                Type escapeType = getVarcharType(node.getEscape(), context);
                coerceType(context, node.getEscape(), escapeType, "Escape for LIKE expression");
            }

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        private Type getVarcharType(Expression value, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = process(value, context);
            if (!(type instanceof VarcharType)) {
                return VARCHAR;
            }
            return type;
        }

        @Override
        protected Type visitSubscriptExpression(SubscriptExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
        }

        @Override
        protected Type visitArrayConstructor(ArrayConstructor node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s", node.getValues());
            Type arrayType = typeManager.getParameterizedType(ARRAY.getName(), ImmutableList.of(type.getTypeSignature()), ImmutableList.of());
            expressionTypes.put(node, arrayType);
            return arrayType;
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            VarcharType type = VarcharType.createVarcharType(SliceUtf8.countCodePoints(node.getSlice()));
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitBinaryLiteral(BinaryLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            expressionTypes.put(node, VARBINARY);
            return VARBINARY;
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                expressionTypes.put(node, INTEGER);
                return INTEGER;
            }

            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            expressionTypes.put(node, DOUBLE);
            return DOUBLE;
        }

        @Override
        protected Type visitDecimalLiteral(DecimalLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            expressionTypes.put(node, parseResult.getType());
            return parseResult.getType();
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitGenericLiteral(GenericLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = typeManager.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (!JSON.equals(type)) {
                try {
                    functionRegistry.getCoercion(VARCHAR, type);
                }
                catch (IllegalArgumentException e) {
                    throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
                }
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type;
            if (timeHasTimeZone(node.getValue())) {
                type = TIME_WITH_TIME_ZONE;
            }
            else {
                type = TIME;
            }
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            try {
                parseTimestampLiteral(session.getTimeZoneKey(), node.getValue());
            }
            catch (Exception e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid timestamp literal", node.getValue());
            }

            Type type;
            if (timestampHasTimeZone(node.getValue())) {
                type = TIMESTAMP_WITH_TIME_ZONE;
            }
            else {
                type = TIMESTAMP;
            }
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH;
            }
            else {
                type = INTERVAL_DAY_TIME;
            }
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, StackableAstVisitorContext<AnalysisContext> context)
        {
            expressionTypes.put(node, UNKNOWN);
            return UNKNOWN;
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, StackableAstVisitorContext<AnalysisContext> context)
        {
            if (node.getWindow().isPresent()) {
                for (Expression expression : node.getWindow().get().getPartitionBy()) {
                    process(expression, context);
                    Type type = expressionTypes.get(expression);
                    if (!type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }

                for (SortItem sortItem : node.getWindow().get().getOrderBy()) {
                    process(sortItem.getSortKey(), context);
                    Type type = expressionTypes.get(sortItem.getSortKey());
                    if (!type.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not orderable, and therefore cannot be used in window function ORDER BY", type);
                    }
                }

                if (node.getWindow().get().getFrame().isPresent()) {
                    WindowFrame frame = node.getWindow().get().getFrame().get();

                    if (frame.getStart().getValue().isPresent()) {
                        Type type = process(frame.getStart().getValue().get(), context);
                        if (!type.equals(INTEGER) && !type.equals(BIGINT)) {
                            throw new SemanticException(TYPE_MISMATCH, node, "Window frame start value type must be INTEGER or BIGINT(actual %s)", type);
                        }
                    }

                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        Type type = process(frame.getEnd().get().getValue().get(), context);
                        if (!type.equals(INTEGER) && !type.equals(BIGINT)) {
                            throw new SemanticException(TYPE_MISMATCH, node, "Window frame end value type must be INTEGER or BIGINT (actual %s)", type);
                        }
                    }
                }
            }

            ImmutableList.Builder<TypeSignature> argumentTypes = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                argumentTypes.add(process(expression, context).getTypeSignature());
            }

            Signature function;
            try {
                function = functionRegistry.resolveFunction(node.getName(), argumentTypes.build(), context.getContext().isApproximate());
            }
            catch (PrestoException e) {
                if (e.getErrorCode().getCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                    throw new SemanticException(SemanticErrorCode.FUNCTION_NOT_FOUND, node, e.getMessage());
                }
                throw e;
            }

            for (int i = 0; i < node.getArguments().size(); i++) {
                Expression expression = node.getArguments().get(i);
                Type type = typeManager.getType(function.getArgumentTypes().get(i));
                requireNonNull(type, format("Type %s not found", function.getArgumentTypes().get(i)));
                if (node.isDistinct() && !type.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", type);
                }
                coerceType(context, expression, type, format("Function %s argument %d", function, i));
            }
            resolvedFunctions.put(node, function);

            Type type = typeManager.getType(function.getReturnType());
            expressionTypes.put(node, type);

            return type;
        }

        @Override
        protected Type visitAtTimeZone(AtTimeZone node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type valueType = process(node.getValue(), context);
            process(node.getTimeZone(), context);
            if (!valueType.equals(TIME_WITH_TIME_ZONE) && !valueType.equals(TIMESTAMP_WITH_TIME_ZONE) && !valueType.equals(TIME) && !valueType.equals(TIMESTAMP)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Type of value must be a time or timestamp with or without time zone (actual %s)", valueType);
            }
            Type resultType = valueType;
            if (valueType.equals(TIME)) {
                resultType = TIME_WITH_TIME_ZONE;
            }
            else if (valueType.equals(TIMESTAMP)) {
                resultType = TIMESTAMP_WITH_TIME_ZONE;
            }
            expressionTypes.put(node, resultType);

            return resultType;
        }

        @Override
        protected Type visitExtract(Extract node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = process(node.getExpression(), context);
            if (!isDateTimeType(type)) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL (actual %s)", type);
            }
            Extract.Field field = node.getField();
            if ((field == TIMEZONE_HOUR || field == TIMEZONE_MINUTE) && !(type.equals(TIME_WITH_TIME_ZONE) || type.equals(TIMESTAMP_WITH_TIME_ZONE))) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract time zone field must have a time zone (actual %s)", type);
            }

            expressionTypes.put(node, BIGINT);
            return BIGINT;
        }

        private boolean isDateTimeType(Type type)
        {
            return type.equals(DATE) ||
                    type.equals(TIME) ||
                    type.equals(TIME_WITH_TIME_ZONE) ||
                    type.equals(TIMESTAMP) ||
                    type.equals(TIMESTAMP_WITH_TIME_ZONE) ||
                    type.equals(INTERVAL_DAY_TIME) ||
                    type.equals(INTERVAL_YEAR_MONTH);
        }

        @Override
        protected Type visitBetweenPredicate(BetweenPredicate node, StackableAstVisitorContext<AnalysisContext> context)
        {
            return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(), node.getMax());
        }

        @Override
        public Type visitTryExpression(TryExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = process(node.getInnerExpression(), context);
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        public Type visitCast(Cast node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = typeManager.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (type.equals(UNKNOWN)) {
                throw new SemanticException(TYPE_MISMATCH, node, "UNKNOWN is not a valid type");
            }

            Type value = process(node.getExpression(), context);
            if (!value.equals(UNKNOWN) && !node.isTypeOnly()) {
                try {
                    functionRegistry.getCoercion(value, type);
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitInPredicate(InPredicate node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Expression value = node.getValue();
            process(value, context);

            Expression valueList = node.getValueList();
            process(valueList, context);

            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;

                coerceToSingleType(context,
                        "IN value and list items must be the same type: %s",
                        ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build());
            }
            else if (valueList instanceof SubqueryExpression) {
                coerceToSingleType(context, node, "value and result of subquery must be of the same type for IN expression: %s vs %s", value, valueList);
            }

            expressionTypes.put(node, BOOLEAN);
            return BOOLEAN;
        }

        @Override
        protected Type visitInListExpression(InListExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = coerceToSingleType(context, "All IN list values must be the same type: %s", node.getValues());

            expressionTypes.put(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            RelationType descriptor = analyzer.process(node.getQuery(), new AnalysisContext(context.getContext(), tupleDescriptor));

            // Subquery should only produce one column
            if (descriptor.getVisibleFieldCount() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SUBQUERY,
                        node,
                        "Multiple columns returned by subquery are not yet supported. Found %s",
                        descriptor.getVisibleFieldCount());
            }

            Optional<Node> previousNode = context.getPreviousNode();
            if (previousNode.isPresent() && previousNode.get() instanceof InPredicate && ((InPredicate) previousNode.get()).getValue() != node) {
                subqueryInPredicates.add((InPredicate) previousNode.get());
            }
            else {
                scalarSubqueries.add(node);
            }

            Type type = Iterables.getOnlyElement(descriptor.getVisibleFields()).getType();
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        public Type visitFieldReference(FieldReference node, StackableAstVisitorContext<AnalysisContext> context)
        {
            Type type = tupleDescriptor.getFieldByIndex(node.getFieldIndex()).getType();
            expressionTypes.put(node, type);
            return type;
        }

        @Override
        protected Type visitExpression(Expression node, StackableAstVisitorContext<AnalysisContext> context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "not yet implemented: " + node.getClass().getName());
        }

        private Type getOperator(StackableAstVisitorContext<AnalysisContext> context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            Signature operatorSignature;
            try {
                operatorSignature = functionRegistry.resolveOperator(operatorType, argumentTypes.build());
            }
            catch (OperatorNotFoundException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "%s", e.getMessage());
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                Type type = typeManager.getType(operatorSignature.getArgumentTypes().get(i));
                coerceType(context, expression, type, format("Operator %s argument %d", operatorSignature, i));
            }

            Type type = typeManager.getType(operatorSignature.getReturnType());
            expressionTypes.put(node, type);

            return type;
        }

        private void coerceType(StackableAstVisitorContext<AnalysisContext> context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            if (!actualType.equals(expectedType)) {
                if (!canCoerce(actualType, expectedType)) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
                }
                expressionCoercions.put(expression, expectedType);
            }
        }

        private Type coerceToSingleType(StackableAstVisitorContext<AnalysisContext> context, Node node, String message, Expression first, Expression second)
        {
            Type firstType = null;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = null;
            if (second != null) {
                secondType = process(second, context);
            }

            if (firstType == null) {
                return secondType;
            }
            if (secondType == null) {
                return firstType;
            }
            if (firstType.equals(secondType)) {
                return firstType;
            }

            // coerce types if possible
            if (canCoerce(firstType, secondType)) {
                expressionCoercions.put(first, secondType);
                return secondType;
            }
            if (canCoerce(secondType, firstType)) {
                expressionCoercions.put(second, firstType);
                return firstType;
            }
            throw new SemanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<AnalysisContext> context, String message, List<Expression> expressions)
        {
            // determine super type
            Type superType = UNKNOWN;
            for (Expression expression : expressions) {
                Optional<Type> newSuperType = typeManager.getCommonSuperType(superType, process(expression, context));
                if (!newSuperType.isPresent()) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                }
                superType = newSuperType.get();
            }

            // verify all expressions can be coerced to the superType
            for (Expression expression : expressions) {
                Type type = process(expression, context);
                if (!type.equals(superType)) {
                    if (!canCoerce(type, superType)) {
                        throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                    }
                    expressionCoercions.put(expression, superType);
                }
            }

            return superType;
        }
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Expression expression)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Iterable<? extends Expression> expressions)
    {
        return analyzeExpressionsWithSymbols(session, metadata, sqlParser, types, expressions).getExpressionTypes();
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Expression expression)
    {
        return getExpressionTypesFromInput(session, metadata, sqlParser, types, ImmutableList.of(expression));
    }

    public static IdentityHashMap<Expression, Type> getExpressionTypesFromInput(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Iterable<? extends Expression> expressions)
    {
        return analyzeExpressionsWithInputs(session, metadata, sqlParser, types, expressions).getExpressionTypes();
    }

    public static ExpressionAnalysis analyzeExpressionsWithSymbols(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Iterable<? extends Expression> expressions)
    {
        List<Field> fields = DependencyExtractor.extractUnique(expressions).stream()
                .map(symbol -> {
                    Type type = types.get(symbol);
                    checkArgument(type != null, "No type for symbol %s", symbol);
                    return Field.newUnqualified(symbol.getName(), type);
                })
                .collect(toImmutableList());

        return analyzeExpressions(session, metadata, sqlParser, new RelationType(fields), expressions);
    }

    private static ExpressionAnalysis analyzeExpressionsWithInputs(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Iterable<? extends Expression> expressions)
    {
        Field[] fields = new Field[types.size()];
        for (Entry<Integer, Type> entry : types.entrySet()) {
            fields[entry.getKey()] = Field.newUnqualified(Optional.empty(), entry.getValue());
        }
        RelationType tupleDescriptor = new RelationType(fields);

        return analyzeExpressions(session, metadata, sqlParser, tupleDescriptor, expressions);
    }

    private static ExpressionAnalysis analyzeExpressions(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            RelationType tupleDescriptor,
            Iterable<? extends Expression> expressions)
    {
        // expressions at this point can not have sub queries so deny all access checks
        // in the future, we will need a full access controller here to verify access to functions
        ExpressionAnalyzer analyzer = create(new Analysis(), session, metadata, sqlParser, new DenyAllAccessControl(), false);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, tupleDescriptor, new AnalysisContext());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getResolvedNames().keySet());
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            RelationType tupleDescriptor,
            Analysis analysis,
            boolean approximateQueriesEnabled,
            AnalysisContext context,
            Expression expression)
    {
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, accessControl, approximateQueriesEnabled);
        analyzer.analyze(expression, tupleDescriptor, context);

        IdentityHashMap<Expression, Type> expressionTypes = analyzer.getExpressionTypes();
        IdentityHashMap<Expression, Type> expressionCoercions = analyzer.getExpressionCoercions();
        IdentityHashMap<FunctionCall, Signature> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions);
        analysis.addFunctionSignatures(resolvedFunctions);

        analysis.addResolvedNames(analyzer.getResolvedNames());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getColumnReferences());
    }

    public static ExpressionAnalyzer create(
            Analysis analysis,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            boolean experimentalSyntaxEnabled)
    {
        return new ExpressionAnalyzer(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                node -> new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, experimentalSyntaxEnabled, Optional.empty()),
                session);
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session)
    {
        return createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                session,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery");
    }

    public static ExpressionAnalyzer createWithoutSubqueries(FunctionRegistry functionRegistry, TypeManager typeManager, Session session, SemanticErrorCode errorCode, String message)
    {
        return new ExpressionAnalyzer(functionRegistry, typeManager, node -> {
            throw new SemanticException(errorCode, node, message);
        }, session);
    }
}
