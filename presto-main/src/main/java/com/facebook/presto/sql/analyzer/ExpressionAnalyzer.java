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
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.DenyAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalParseResult;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StackableAstVisitor;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.type.FunctionType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.slice.SliceUtf8;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.isLegacyRowFieldOrdinalAccessEnabled;
import static com.facebook.presto.spi.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.missingAttributeException;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.timeHasTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampHasTimeZone;
import static com.facebook.presto.util.LegacyRowFieldOrdinalAccessUtil.parseAnonymousRowFieldOrdinalAccess;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    private final FunctionRegistry functionRegistry;
    private final TypeManager typeManager;
    private final Function<Node, StatementAnalyzer> statementAnalyzerFactory;
    private final Map<Symbol, Type> symbolTypes;
    private final boolean isDescribe;
    private final boolean legacyRowFieldOrdinalAccess;

    private final Map<NodeRef<FunctionCall>, Signature> resolvedFunctions = new LinkedHashMap<>();
    private final Set<NodeRef<SubqueryExpression>> scalarSubqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();
    private final Set<NodeRef<FunctionCall>> windowFunctions = new LinkedHashSet<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

    private final Session session;
    private final List<Expression> parameters;

    public ExpressionAnalyzer(
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Function<Node, StatementAnalyzer> statementAnalyzerFactory,
            Session session,
            Map<Symbol, Type> symbolTypes,
            List<Expression> parameters,
            boolean isDescribe)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.symbolTypes = ImmutableMap.copyOf(requireNonNull(symbolTypes, "symbolTypes is null"));
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.legacyRowFieldOrdinalAccess = isLegacyRowFieldOrdinalAccessEnabled(session);
    }

    public Map<NodeRef<FunctionCall>, Signature> getResolvedFunctions()
    {
        return unmodifiableMap(resolvedFunctions);
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public Type setExpressionType(Expression expression, Type type)
    {
        requireNonNull(expression, "expression cannot be null");
        requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private Type getExpressionType(Expression expression)
    {
        requireNonNull(expression, "expression cannot be null");

        Type type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, Type> getExpressionCoercions()
    {
        return unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates()
    {
        return unmodifiableSet(subqueryInPredicates);
    }

    public Map<NodeRef<Expression>, FieldId> getColumnReferences()
    {
        return unmodifiableMap(columnReferences);
    }

    public Map<NodeRef<Identifier>, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return unmodifiableMap(lambdaArgumentReferences);
    }

    public Type analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope)));
    }

    private Type analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    public Set<NodeRef<SubqueryExpression>> getScalarSubqueries()
    {
        return unmodifiableSet(scalarSubqueries);
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
    {
        return unmodifiableSet(existsSubqueries);
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return unmodifiableSet(quantifiedComparisons);
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions()
    {
        return unmodifiableSet(windowFunctions);
    }

    public Multimap<QualifiedObjectName, String> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    private class Visitor
            extends StackableAstVisitor<Type, Context>
    {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;

        public Visitor(Scope baseScope)
        {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
        }

        @Override
        public Type process(Node node, @Nullable StackableAstVisitorContext<Context> context)
        {
            if (node instanceof Expression) {
                // don't double process a node
                Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        protected Type visitRow(Row node, StackableAstVisitorContext<Context> context)
        {
            List<Type> types = node.getItems().stream()
                    .map((child) -> process(child, context))
                    .collect(toImmutableList());

            Type type = RowType.anonymous(types);
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitCurrentTime(CurrentTime node, StackableAstVisitorContext<Context> context)
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

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitSymbolReference(SymbolReference node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                Optional<ResolvedField> resolvedField = context.getContext().getScope().tryResolveField(node, QualifiedName.of(node.getName()));
                if (resolvedField.isPresent() && context.getContext().getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
                    return setExpressionType(node, resolvedField.get().getType());
                }
            }
            Type type = symbolTypes.get(Symbol.from(node));
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context)
        {
            ResolvedField resolvedField = context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context)
        {
            return handleResolvedField(node, FieldId.from(resolvedField), resolvedField.getField(), context);
        }

        private Type handleResolvedField(Expression node, FieldId fieldId, Field field, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getContext().getFieldToLambdaArgumentDeclaration().get(fieldId);
                if (lambdaArgumentDeclaration != null) {
                    // Lambda argument reference is not a column reference
                    lambdaArgumentReferences.put(NodeRef.of((Identifier) node), lambdaArgumentDeclaration);
                    return setExpressionType(node, field.getType());
                }
            }

            if (field.getOriginTable().isPresent() && field.getName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getName().get());
            }

            FieldId previous = columnReferences.put(NodeRef.of(node), fieldId);
            checkState(previous == null, "%s already known to refer to %s", node, previous);
            return setExpressionType(node, field.getType());
        }

        @Override
        protected Type visitDereferenceExpression(DereferenceExpression node, StackableAstVisitorContext<Context> context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                Scope scope = context.getContext().getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                if (!scope.isColumnReference(qualifiedName)) {
                    throw missingAttributeException(node, qualifiedName);
                }
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType)) {
                throw new SemanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            RowType rowType = (RowType) baseType;
            String fieldName = node.getField().getValue();

            Type rowFieldType = null;
            for (RowType.Field rowField : rowType.getFields()) {
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    rowFieldType = rowField.getType();
                    break;
                }
            }

            if (legacyRowFieldOrdinalAccess && rowFieldType == null) {
                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, rowType.getFields());
                if (rowIndex.isPresent()) {
                    rowFieldType = rowType.getFields().get(rowIndex.getAsInt()).getType();
                }
            }

            if (rowFieldType == null) {
                throw missingAttributeException(node);
            }

            return setExpressionType(node, rowFieldType);
        }

        @Override
        protected Type visitNotExpression(NotExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getValue(), BOOLEAN, "Value of logical NOT expression");

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitLogicalBinaryExpression(LogicalBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getLeft(), BOOLEAN, "Left side of logical expression");
            coerceType(context, node.getRight(), BOOLEAN, "Right side of logical expression");

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            OperatorType operatorType = OperatorType.valueOf(node.getType().name());
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitIsNullPredicate(IsNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitIsNotNullPredicate(IsNotNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitNullIfExpression(NullIfExpression node, StackableAstVisitorContext<Context> context)
        {
            Type firstType = process(node.getFirst(), context);
            Type secondType = process(node.getSecond(), context);

            if (!typeManager.getCommonSuperType(firstType, secondType).isPresent()) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            return setExpressionType(node, firstType);
        }

        @Override
        protected Type visitIfExpression(IfExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getCondition(), BOOLEAN, "IF condition");

            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same: %s vs %s", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitSearchedCaseExpression(SearchedCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceType(context, whenClause.getOperand(), BOOLEAN, "CASE WHEN clause");
            }

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        protected Type visitSimpleCaseExpression(SimpleCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceToSingleType(context, whenClause, "CASE operand type does not match WHEN clause operand type: %s vs %s", node.getOperand(), whenClause.getOperand());
            }

            Type type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
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
        protected Type visitCoalesceExpression(CoalesceExpression node, StackableAstVisitorContext<Context> context)
        {
            Type type = coerceToSingleType(context, "All COALESCE operands must be the same type: %s", node.getOperands());

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitArithmeticUnary(ArithmeticUnaryExpression node, StackableAstVisitorContext<Context> context)
        {
            switch (node.getSign()) {
                case PLUS:
                    Type type = process(node.getValue(), context);

                    if (!type.equals(DOUBLE) && !type.equals(REAL) && !type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw new SemanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    return setExpressionType(node, type);
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected Type visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getType().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected Type visitLikePredicate(LikePredicate node, StackableAstVisitorContext<Context> context)
        {
            Type valueType = getVarcharType(node.getValue(), context);
            Type patternType = getVarcharType(node.getPattern(), context);
            coerceType(context, node.getValue(), valueType, "Left side of LIKE expression");
            coerceType(context, node.getPattern(), patternType, "Pattern for LIKE expression");
            if (node.getEscape() != null) {
                Type escapeType = getVarcharType(node.getEscape(), context);
                coerceType(context, node.getEscape(), escapeType, "Escape for LIKE expression");
            }

            return setExpressionType(node, BOOLEAN);
        }

        private Type getVarcharType(Expression value, StackableAstVisitorContext<Context> context)
        {
            Type type = process(value, context);
            if (!(type instanceof VarcharType)) {
                return VARCHAR;
            }
            return type;
        }

        @Override
        protected Type visitSubscriptExpression(SubscriptExpression node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
        }

        @Override
        protected Type visitArrayConstructor(ArrayConstructor node, StackableAstVisitorContext<Context> context)
        {
            Type type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s", node.getValues());
            Type arrayType = typeManager.getParameterizedType(ARRAY.getName(), ImmutableList.of(TypeSignatureParameter.of(type.getTypeSignature())));
            return setExpressionType(node, arrayType);
        }

        @Override
        protected Type visitStringLiteral(StringLiteral node, StackableAstVisitorContext<Context> context)
        {
            VarcharType type = VarcharType.createVarcharType(SliceUtf8.countCodePoints(node.getSlice()));
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitCharLiteral(CharLiteral node, StackableAstVisitorContext<Context> context)
        {
            CharType type = CharType.createCharType(node.getValue().length());
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitBinaryLiteral(BinaryLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARBINARY);
        }

        @Override
        protected Type visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER);
            }

            return setExpressionType(node, BIGINT);
        }

        @Override
        protected Type visitDoubleLiteral(DoubleLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, DOUBLE);
        }

        @Override
        protected Type visitDecimalLiteral(DecimalLiteral node, StackableAstVisitorContext<Context> context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return setExpressionType(node, parseResult.getType());
        }

        @Override
        protected Type visitBooleanLiteral(BooleanLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitGenericLiteral(GenericLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                type = typeManager.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
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

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitTimeLiteral(TimeLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            if (timeHasTimeZone(node.getValue())) {
                type = TIME_WITH_TIME_ZONE;
            }
            else {
                type = TIME;
            }
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitTimestampLiteral(TimestampLiteral node, StackableAstVisitorContext<Context> context)
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
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitIntervalLiteral(IntervalLiteral node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH;
            }
            else {
                type = INTERVAL_DAY_TIME;
            }
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitNullLiteral(NullLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, UNKNOWN);
        }

        @Override
        protected Type visitFunctionCall(FunctionCall node, StackableAstVisitorContext<Context> context)
        {
            if (node.getWindow().isPresent()) {
                for (Expression expression : node.getWindow().get().getPartitionBy()) {
                    process(expression, context);
                    Type type = getExpressionType(expression);
                    if (!type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }

                for (SortItem sortItem : getSortItemsFromOrderBy(node.getWindow().get().getOrderBy())) {
                    process(sortItem.getSortKey(), context);
                    Type type = getExpressionType(sortItem.getSortKey());
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

                windowFunctions.add(NodeRef.of(node));
            }

            if (node.getFilter().isPresent()) {
                Expression expression = node.getFilter().get();
                process(expression, context);
            }

            ImmutableList.Builder<TypeSignatureProvider> argumentTypesBuilder = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                if (expression instanceof LambdaExpression || expression instanceof BindExpression) {
                    argumentTypesBuilder.add(new TypeSignatureProvider(
                            types -> {
                                ExpressionAnalyzer innerExpressionAnalyzer = new ExpressionAnalyzer(
                                        functionRegistry,
                                        typeManager,
                                        statementAnalyzerFactory,
                                        session,
                                        symbolTypes,
                                        parameters,
                                        isDescribe);
                                if (context.getContext().isInLambda()) {
                                    for (LambdaArgumentDeclaration argument : context.getContext().getFieldToLambdaArgumentDeclaration().values()) {
                                        innerExpressionAnalyzer.setExpressionType(argument, getExpressionType(argument));
                                    }
                                }
                                return innerExpressionAnalyzer.analyze(expression, baseScope, context.getContext().expectingLambda(types)).getTypeSignature();
                            }));
                }
                else {
                    argumentTypesBuilder.add(new TypeSignatureProvider(process(expression, context).getTypeSignature()));
                }
            }

            ImmutableList<TypeSignatureProvider> argumentTypes = argumentTypesBuilder.build();
            Signature function = resolveFunction(node, argumentTypes, functionRegistry);

            if (node.getOrderBy().isPresent()) {
                for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
                    Type sortKeyType = process(sortItem.getSortKey(), context);
                    if (!sortKeyType.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "ORDER BY can only be applied to orderable types (actual: %s)", sortKeyType.getDisplayName());
                    }
                }
            }

            for (int i = 0; i < node.getArguments().size(); i++) {
                Expression expression = node.getArguments().get(i);
                Type expectedType = typeManager.getType(function.getArgumentTypes().get(i));
                requireNonNull(expectedType, format("Type %s not found", function.getArgumentTypes().get(i)));
                if (node.isDistinct() && !expectedType.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
                }
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, new StackableAstVisitorContext<>(context.getContext().expectingLambda(expectedFunctionType.getArgumentTypes())));
                }
                else {
                    Type actualType = typeManager.getType(argumentTypes.get(i).getTypeSignature());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            Type type = typeManager.getType(function.getReturnType());
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitAtTimeZone(AtTimeZone node, StackableAstVisitorContext<Context> context)
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

            return setExpressionType(node, resultType);
        }

        @Override
        protected Type visitCurrentUser(CurrentUser node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR);
        }

        @Override
        protected Type visitParameter(Parameter node, StackableAstVisitorContext<Context> context)
        {
            if (isDescribe) {
                return setExpressionType(node, UNKNOWN);
            }
            if (parameters.size() == 0) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node, "query takes no parameters");
            }
            if (node.getPosition() >= parameters.size()) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node, "invalid parameter index %s, max value is %s", node.getPosition(), parameters.size() - 1);
            }

            Type resultType = process(parameters.get(node.getPosition()), context);
            return setExpressionType(node, resultType);
        }

        @Override
        protected Type visitExtract(Extract node, StackableAstVisitorContext<Context> context)
        {
            Type type = process(node.getExpression(), context);
            if (!isDateTimeType(type)) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL (actual %s)", type);
            }
            Extract.Field field = node.getField();
            if ((field == TIMEZONE_HOUR || field == TIMEZONE_MINUTE) && !(type.equals(TIME_WITH_TIME_ZONE) || type.equals(TIMESTAMP_WITH_TIME_ZONE))) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract time zone field must have a time zone (actual %s)", type);
            }

            return setExpressionType(node, BIGINT);
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
        protected Type visitBetweenPredicate(BetweenPredicate node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(), node.getMax());
        }

        @Override
        public Type visitTryExpression(TryExpression node, StackableAstVisitorContext<Context> context)
        {
            Type type = process(node.getInnerExpression(), context);
            return setExpressionType(node, type);
        }

        @Override
        public Type visitCast(Cast node, StackableAstVisitorContext<Context> context)
        {
            Type type;
            try {
                type = typeManager.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
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

            return setExpressionType(node, type);
        }

        @Override
        protected Type visitInPredicate(InPredicate node, StackableAstVisitorContext<Context> context)
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

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitInListExpression(InListExpression node, StackableAstVisitorContext<Context> context)
        {
            Type type = coerceToSingleType(context, "All IN list values must be the same type: %s", node.getValues());

            setExpressionType(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected Type visitSubqueryExpression(SubqueryExpression node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Lambda expression cannot contain subqueries");
            }
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .build();
            Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

            // Subquery should only produce one column
            if (queryScope.getRelationType().getVisibleFieldCount() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SUBQUERY,
                        node,
                        "Multiple columns returned by subquery are not yet supported. Found %s",
                        queryScope.getRelationType().getVisibleFieldCount());
            }

            Node previousNode = context.getPreviousNode().orElse(null);
            if (previousNode instanceof InPredicate && ((InPredicate) previousNode).getValue() != node) {
                subqueryInPredicates.add(NodeRef.of((InPredicate) previousNode));
            }
            else if (previousNode instanceof QuantifiedComparisonExpression) {
                quantifiedComparisons.add(NodeRef.of((QuantifiedComparisonExpression) previousNode));
            }
            else {
                scalarSubqueries.add(NodeRef.of(node));
            }

            Type type = getOnlyElement(queryScope.getRelationType().getVisibleFields()).getType();
            return setExpressionType(node, type);
        }

        @Override
        protected Type visitExists(ExistsPredicate node, StackableAstVisitorContext<Context> context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            Scope subqueryScope = Scope.builder().withParent(context.getContext().getScope()).build();
            analyzer.analyze(node.getSubquery(), subqueryScope);

            existsSubqueries.add(NodeRef.of(node));

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        protected Type visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            Expression value = node.getValue();
            process(value, context);

            Expression subquery = node.getSubquery();
            process(subquery, context);

            Type comparisonType = coerceToSingleType(context, node, "Value expression and result of subquery must be of the same type for quantified comparison: %s vs %s", value, subquery);

            switch (node.getComparisonType()) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (!comparisonType.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    if (!comparisonType.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be comparable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                default:
                    throw new IllegalStateException(format("Unexpected comparison type: %s", node.getComparisonType()));
            }

            return setExpressionType(node, BOOLEAN);
        }

        @Override
        public Type visitFieldReference(FieldReference node, StackableAstVisitorContext<Context> context)
        {
            Field field = baseScope.getRelationType().getFieldByIndex(node.getFieldIndex());
            return handleResolvedField(node, new FieldId(baseScope.getRelationId(), node.getFieldIndex()), field, context);
        }

        @Override
        protected Type visitLambdaExpression(LambdaExpression node, StackableAstVisitorContext<Context> context)
        {
            verifyNoAggregateWindowOrGroupingFunctions(functionRegistry, node.getBody(), "Lambda expression");
            if (!context.getContext().isExpectingLambda()) {
                throw new SemanticException(STANDALONE_LAMBDA, node, "Lambda expression should always be used inside a function");
            }

            List<Type> types = context.getContext().getFunctionInputTypes();
            List<LambdaArgumentDeclaration> lambdaArguments = node.getArguments();

            if (types.size() != lambdaArguments.size()) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node,
                        format("Expected a lambda that takes %s argument(s) but got %s", types.size(), lambdaArguments.size()));
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < lambdaArguments.size(); i++) {
                LambdaArgumentDeclaration lambdaArgument = lambdaArguments.get(i);
                Type type = types.get(i);
                fields.add(com.facebook.presto.sql.analyzer.Field.newUnqualified(lambdaArgument.getName().getValue(), type));
                setExpressionType(lambdaArgument, type);
            }

            Scope lambdaScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .withRelationType(RelationId.of(node), new RelationType(fields.build()))
                    .build();

            ImmutableMap.Builder<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration = ImmutableMap.builder();
            if (context.getContext().isInLambda()) {
                fieldToLambdaArgumentDeclaration.putAll(context.getContext().getFieldToLambdaArgumentDeclaration());
            }
            for (LambdaArgumentDeclaration lambdaArgument : lambdaArguments) {
                ResolvedField resolvedField = lambdaScope.resolveField(lambdaArgument, QualifiedName.of(lambdaArgument.getName().getValue()));
                fieldToLambdaArgumentDeclaration.put(FieldId.from(resolvedField), lambdaArgument);
            }

            Type returnType = process(node.getBody(), new StackableAstVisitorContext<>(Context.inLambda(lambdaScope, fieldToLambdaArgumentDeclaration.build())));
            FunctionType functionType = new FunctionType(types, returnType);
            return setExpressionType(node, functionType);
        }

        @Override
        protected Type visitBindExpression(BindExpression node, StackableAstVisitorContext<Context> context)
        {
            verify(context.getContext().isExpectingLambda(), "bind expression found when lambda is not expected");

            StackableAstVisitorContext<Context> innerContext = new StackableAstVisitorContext<>(context.getContext().notExpectingLambda());
            ImmutableList.Builder<Type> functionInputTypesBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                functionInputTypesBuilder.add(process(value, innerContext));
            }
            functionInputTypesBuilder.addAll(context.getContext().getFunctionInputTypes());
            List<Type> functionInputTypes = functionInputTypesBuilder.build();

            FunctionType functionType = (FunctionType) process(node.getFunction(), new StackableAstVisitorContext<>(context.getContext().expectingLambda(functionInputTypes)));

            List<Type> argumentTypes = functionType.getArgumentTypes();
            int numCapturedValues = node.getValues().size();
            verify(argumentTypes.size() == functionInputTypes.size());
            for (int i = 0; i < numCapturedValues; i++) {
                verify(functionInputTypes.get(i).equals(argumentTypes.get(i)));
            }

            FunctionType result = new FunctionType(argumentTypes.subList(numCapturedValues, argumentTypes.size()), functionType.getReturnType());
            return setExpressionType(node, result);
        }

        @Override
        protected Type visitExpression(Expression node, StackableAstVisitorContext<Context> context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Type visitNode(Node node, StackableAstVisitorContext<Context> context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Type visitGroupingOperation(GroupingOperation node, StackableAstVisitorContext<Context> context)
        {
            if (node.getGroupingColumns().size() > MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT) {
                throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, node, String.format("GROUPING supports up to %d column arguments", MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT));
            }

            for (Expression columnArgument : node.getGroupingColumns()) {
                process(columnArgument, context);
            }

            if (node.getGroupingColumns().size() <= MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER) {
                return setExpressionType(node, INTEGER);
            }
            else {
                return setExpressionType(node, BIGINT);
            }
        }

        private Type getOperator(StackableAstVisitorContext<Context> context, Expression node, OperatorType operatorType, Expression... arguments)
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
            catch (PrestoException e) {
                if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                    throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
                }
                throw e;
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                Type type = typeManager.getType(operatorSignature.getArgumentTypes().get(i));
                coerceType(context, expression, type, format("Operator %s argument %d", operatorSignature, i));
            }

            Type type = typeManager.getType(operatorSignature.getReturnType());
            return setExpressionType(node, type);
        }

        private void coerceType(Expression expression, Type actualType, Type expectedType, String message)
        {
            if (!actualType.equals(expectedType)) {
                if (!typeManager.canCoerce(actualType, expectedType)) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
                }
                addOrReplaceExpressionCoercion(expression, actualType, expectedType);
            }
        }

        private void coerceType(StackableAstVisitorContext<Context> context, Expression expression, Type expectedType, String message)
        {
            Type actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, Node node, String message, Expression first, Expression second)
        {
            Type firstType = UNKNOWN;
            if (first != null) {
                firstType = process(first, context);
            }
            Type secondType = UNKNOWN;
            if (second != null) {
                secondType = process(second, context);
            }

            // coerce types if possible
            Optional<Type> superTypeOptional = typeManager.getCommonSuperType(firstType, secondType);
            if (superTypeOptional.isPresent()
                    && typeManager.canCoerce(firstType, superTypeOptional.get())
                    && typeManager.canCoerce(secondType, superTypeOptional.get())) {
                Type superType = superTypeOptional.get();
                if (!firstType.equals(superType)) {
                    addOrReplaceExpressionCoercion(first, firstType, superType);
                }
                if (!secondType.equals(superType)) {
                    addOrReplaceExpressionCoercion(second, secondType, superType);
                }
                return superType;
            }

            throw new SemanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private Type coerceToSingleType(StackableAstVisitorContext<Context> context, String message, List<Expression> expressions)
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
                    if (!typeManager.canCoerce(type, superType)) {
                        throw new SemanticException(TYPE_MISMATCH, expression, message, superType);
                    }
                    addOrReplaceExpressionCoercion(expression, type, superType);
                }
            }

            return superType;
        }

        private void addOrReplaceExpressionCoercion(Expression expression, Type type, Type superType)
        {
            NodeRef<Expression> ref = NodeRef.of(expression);
            expressionCoercions.put(ref, superType);
            if (typeManager.isTypeOnlyCoercion(type, superType)) {
                typeOnlyCoercions.add(ref);
            }
            else if (typeOnlyCoercions.contains(ref)) {
                typeOnlyCoercions.remove(ref);
            }
        }
    }

    private static class Context
    {
        private final Scope scope;

        // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

        // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
        // Empty list represents expecting a lambda with no arguments.
        private final List<Type> functionInputTypes;
        // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
        // Empty map means that the all lambda expressions surrounding the current node has no arguments.
        private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

        private Context(
                Scope scope,
                List<Type> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
        }

        public static Context notInLambda(Scope scope)
        {
            return new Context(scope, null, null);
        }

        public static Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"));
        }

        public Context expectingLambda(List<Type> functionInputTypes)
        {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration);
        }

        public Context notExpectingLambda()
        {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration);
        }

        Scope getScope()
        {
            return scope;
        }

        public boolean isInLambda()
        {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isExpectingLambda()
        {
            return functionInputTypes != null;
        }

        public Map<FieldId, LambdaArgumentDeclaration> getFieldToLambdaArgumentDeclaration()
        {
            checkState(isInLambda());
            return fieldToLambdaArgumentDeclaration;
        }

        public List<Type> getFunctionInputTypes()
        {
            checkState(isExpectingLambda());
            return functionInputTypes;
        }
    }

    public static Signature resolveFunction(FunctionCall node, List<TypeSignatureProvider> argumentTypes, FunctionRegistry functionRegistry)
    {
        try {
            return functionRegistry.resolveFunction(node.getName(), argumentTypes);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.FUNCTION_NOT_FOUND, node, e.getMessage());
            }
            if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
            }
            throw e;
        }
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Expression expression,
            List<Expression> parameters)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, expression, parameters, false);
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Expression expression,
            List<Expression> parameters,
            boolean isDescribe)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, ImmutableList.of(expression), parameters, isDescribe);
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            boolean isDescribe)
    {
        return analyzeExpressionsWithSymbols(session, metadata, sqlParser, types, expressions, parameters, isDescribe).getExpressionTypes();
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypesFromInput(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Expression expression,
            List<Expression> parameters)
    {
        return getExpressionTypesFromInput(session, metadata, sqlParser, types, ImmutableList.of(expression), parameters);
    }

    public static Map<NodeRef<Expression>, Type> getExpressionTypesFromInput(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Iterable<Expression> expressions,
            List<Expression> parameters)
    {
        return analyzeExpressionsWithInputs(session, metadata, sqlParser, types, expressions, parameters).getExpressionTypes();
    }

    public static ExpressionAnalysis analyzeExpressionsWithSymbols(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Symbol, Type> types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            boolean isDescribe)
    {
        return analyzeExpressions(session, metadata, sqlParser, new RelationType(), types, expressions, parameters, isDescribe);
    }

    private static ExpressionAnalysis analyzeExpressionsWithInputs(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            Map<Integer, Type> types,
            Iterable<Expression> expressions,
            List<Expression> parameters)
    {
        Field[] fields = new Field[types.size()];
        for (Entry<Integer, Type> entry : types.entrySet()) {
            fields[entry.getKey()] = com.facebook.presto.sql.analyzer.Field.newUnqualified(Optional.empty(), entry.getValue());
        }
        RelationType tupleDescriptor = new RelationType(fields);

        return analyzeExpressions(session, metadata, sqlParser, tupleDescriptor, ImmutableMap.of(), expressions, parameters);
    }

    public static ExpressionAnalysis analyzeExpressions(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            RelationType tupleDescriptor,
            Map<Symbol, Type> types,
            Iterable<? extends Expression> expressions,
            List<Expression> parameters)
    {
        return analyzeExpressions(session, metadata, sqlParser, tupleDescriptor, types, expressions, parameters, false);
    }

    private static ExpressionAnalysis analyzeExpressions(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            RelationType tupleDescriptor,
            Map<Symbol, Type> types,
            Iterable<? extends Expression> expressions,
            List<Expression> parameters,
            boolean isDescribe)
    {
        // expressions at this point can not have sub queries so deny all access checks
        // in the future, we will need a full access controller here to verify access to functions
        Analysis analysis = new Analysis(null, parameters, isDescribe);
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, new DenyAllAccessControl(), types);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, Scope.builder().withRelationType(RelationId.anonymous(), tupleDescriptor).build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            Scope scope,
            Analysis analysis,
            Expression expression)
    {
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, accessControl, ImmutableMap.of());
        analyzer.analyze(expression, scope);

        Map<NodeRef<Expression>, Type> expressionTypes = analyzer.getExpressionTypes();
        Map<NodeRef<Expression>, Type> expressionCoercions = analyzer.getExpressionCoercions();
        Set<NodeRef<Expression>> typeOnlyCoercions = analyzer.getTypeOnlyCoercions();
        Map<NodeRef<FunctionCall>, Signature> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
        analysis.addFunctionSignatures(resolvedFunctions);
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        analysis.addTableColumnReferences(analyzer.getTableColumnReferences());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalyzer create(
            Analysis analysis,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl)
    {
        return create(analysis, session, metadata, sqlParser, accessControl, ImmutableMap.of());
    }

    public static ExpressionAnalyzer create(
            Analysis analysis,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            Map<Symbol, Type> types)
    {
        return new ExpressionAnalyzer(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                node -> new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session),
                session,
                types,
                analysis.getParameters(),
                analysis.isDescribe());
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters)
    {
        return createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                false);
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters, boolean isDescribe)
    {
        return createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            List<Expression> parameters,
            SemanticErrorCode errorCode,
            String message,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                functionRegistry,
                typeManager,
                session,
                ImmutableMap.of(),
                parameters,
                node -> new SemanticException(errorCode, node, message),
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            Map<Symbol, Type> symbolTypes,
            List<Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            boolean isDescribe)
    {
        return new ExpressionAnalyzer(
                functionRegistry,
                typeManager,
                node -> {
                    throw statementAnalyzerRejection.apply(node);
                },
                session,
                symbolTypes,
                parameters,
                isDescribe);
    }
}
