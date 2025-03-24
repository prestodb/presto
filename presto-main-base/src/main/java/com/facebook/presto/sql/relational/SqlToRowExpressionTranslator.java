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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.QuantifiedComparisonExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.UnresolvedSymbolExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
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
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
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
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Pattern;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.isEnumType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.TRY_CAST;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.NULL_IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.resolveEnumLiteral;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.inSubquery;
import static com.facebook.presto.sql.relational.Expressions.quantifiedComparison;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.facebook.presto.sql.tree.DereferenceExpression.getQualifiedName;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.facebook.presto.util.LegacyRowFieldOrdinalAccessUtil.parseAnonymousRowFieldOrdinalAccess;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SqlToRowExpressionTranslator
{
    private static final Pattern LIKE_PREFIX_MATCH_PATTERN = Pattern.compile("^[^%_]*%$");
    private static final Pattern LIKE_SUFFIX_MATCH_PATTERN = Pattern.compile("^%[^%_]*$");
    private static final Pattern LIKE_SIMPLE_EXISTS_PATTERN = Pattern.compile("^%[^%_]*%$");

    private SqlToRowExpressionTranslator() {}

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<VariableReferenceExpression, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Session session)
    {
        return translate(
                expression,
                types,
                layout,
                functionAndTypeManager,
                session,
                new Context());
    }

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<VariableReferenceExpression, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            Context context)
    {
        return translate(
                expression,
                types,
                layout,
                functionAndTypeManager,
                Optional.of(session.getUser()),
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                session.getSessionFunctions(),
                context);
    }

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<VariableReferenceExpression, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Optional<String> user,
            Optional<TransactionId> transactionId,
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            Context context)
    {
        Visitor visitor = new Visitor(
                types,
                layout,
                functionAndTypeManager,
                user,
                transactionId,
                sqlFunctionProperties,
                sessionFunctions);
        RowExpression result = visitor.process(expression, context);
        requireNonNull(result, "translated expression is null");
        return result;
    }

    public static class Context
    {
        private final Map<Expression, RowExpression> rowExpressionMap = new IdentityHashMap<>();
        private final Map<RowExpression, Expression> expressionMap = new IdentityHashMap<>();

        public Context() {}

        public Map<Expression, RowExpression> getRowExpressionMap()
        {
            return rowExpressionMap;
        }

        public Map<RowExpression, Expression> getExpressionMap()
        {
            return expressionMap;
        }

        public void put(Expression expression, RowExpression rowExpression)
        {
            rowExpressionMap.put(expression, rowExpression);
            expressionMap.put(rowExpression, expression);
        }
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Context>
    {
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<VariableReferenceExpression, Integer> layout;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionAndTypeResolver functionAndTypeResolver;
        private final Optional<String> user;
        private final Optional<TransactionId> transactionId;
        private final SqlFunctionProperties sqlFunctionProperties;
        private final Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions;
        private final FunctionResolution functionResolution;

        private Visitor(
                Map<NodeRef<Expression>, Type> types,
                Map<VariableReferenceExpression, Integer> layout,
                FunctionAndTypeManager functionAndTypeManager,
                Optional<String> user,
                Optional<TransactionId> transactionId,
                SqlFunctionProperties sqlFunctionProperties,
                Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
        {
            this.types = requireNonNull(types, "types is null");
            this.layout = requireNonNull(layout);
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager);
            this.functionAndTypeResolver = functionAndTypeManager.getFunctionAndTypeResolver();
            this.user = requireNonNull(user);
            this.transactionId = requireNonNull(transactionId);
            this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties);
            this.functionResolution = new FunctionResolution(functionAndTypeResolver);
            this.sessionFunctions = requireNonNull(sessionFunctions);
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        public RowExpression process(Node node, Context context)
        {
            if (!(node instanceof Expression)) {
                throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
            }
            Expression expression = (Expression) node;
            if (context.getRowExpressionMap().containsKey(expression)) {
                return context.getRowExpressionMap().get(expression);
            }

            RowExpression rowExpression = super.process(expression, context);
            context.put(expression, rowExpression);
            return rowExpression;
        }

        @Override
        protected RowExpression visitExpression(Expression node, Context context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitIdentifier(Identifier node, Context context)
        {
            // identifier should never be reachable with the exception of lambda within VALUES (#9711)
            return new VariableReferenceExpression(getSourceLocation(node), node.getValue(), getType(node));
        }

        @Override
        protected RowExpression visitFieldReference(FieldReference node, Context context)
        {
            return field(getSourceLocation(node), node.getFieldIndex(), getType(node));
        }

        @Override
        protected RowExpression visitNullLiteral(NullLiteral node, Context context)
        {
            return constantNull(getSourceLocation(node), UnknownType.UNKNOWN);
        }

        @Override
        protected RowExpression visitBooleanLiteral(BooleanLiteral node, Context context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected RowExpression visitLongLiteral(LongLiteral node, Context context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Context context)
        {
            return constant(node.getValue(), functionAndTypeManager.getType(DOUBLE.getTypeSignature()));
        }

        @Override
        protected RowExpression visitDecimalLiteral(DecimalLiteral node, Context context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Context context)
        {
            return constant(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
        }

        @Override
        protected RowExpression visitCharLiteral(CharLiteral node, Context context)
        {
            return constant(node.getSlice(), createCharType(node.getValue().length()));
        }

        @Override
        protected RowExpression visitBinaryLiteral(BinaryLiteral node, Context context)
        {
            return constant(node.getValue(), VARBINARY);
        }

        @Override
        protected RowExpression visitEnumLiteral(EnumLiteral node, Context context)
        {
            Type type;
            try {
                type = functionAndTypeResolver.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            return constant(node.getValue(), type);
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Context context)
        {
            Type type;
            try {
                type = functionAndTypeResolver.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            try {
                if (TINYINT.equals(type)) {
                    return constant((long) Byte.parseByte(node.getValue()), TINYINT);
                }
                else if (SMALLINT.equals(type)) {
                    return constant((long) Short.parseShort(node.getValue()), SMALLINT);
                }
                else if (INTEGER.equals(type)) {
                    return constant((long) Integer.parseInt(node.getValue()), INTEGER);
                }
                else if (BIGINT.equals(type)) {
                    return constant(Long.parseLong(node.getValue()), BIGINT);
                }
                else if (INTEGER.equals(type)) {
                    return constant(Long.parseLong(node.getValue()), INTEGER);
                }
            }
            catch (NumberFormatException e) {
                throw new SemanticException(SemanticErrorCode.INVALID_LITERAL, node, format("Invalid formatted generic %s literal: %s", type, node));
            }

            if (JSON.equals(type)) {
                return call(
                        getSourceLocation(node),
                        "json_parse",
                        functionAndTypeResolver.lookupFunction("json_parse", fromTypes(VARCHAR)),
                        getType(node),
                        constant(utf8Slice(node.getValue()), VARCHAR));
            }

            return call(
                    getSourceLocation(node),
                    CAST.name(),
                    functionAndTypeResolver.lookupCast("CAST", VARCHAR, getType(node)),
                    getType(node),
                    constant(utf8Slice(node.getValue()), VARCHAR));
        }

        @Override
        protected RowExpression visitTimeLiteral(TimeLiteral node, Context context)
        {
            long value;
            if (getType(node).equals(TIME_WITH_TIME_ZONE)) {
                value = parseTimeWithTimeZone(node.getValue());
            }
            else {
                if (sqlFunctionProperties.isLegacyTimestamp()) {
                    // parse in time zone of client
                    value = parseTimeWithoutTimeZone(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
                }
                else {
                    value = parseTimeWithoutTimeZone(node.getValue());
                }
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitTimestampLiteral(TimestampLiteral node, Context context)
        {
            long value;
            if (sqlFunctionProperties.isLegacyTimestamp()) {
                value = parseTimestampLiteral(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
            }
            else {
                value = parseTimestampLiteral(node.getValue());
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitIntervalLiteral(IntervalLiteral node, Context context)
        {
            long value;
            if (node.isYearToMonth()) {
                value = node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                value = node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            return constant(value, getType(node));
        }

        @Override
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Context context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    getSourceLocation(node),
                    node.getOperator().name(),
                    functionResolution.comparisonFunction(node.getOperator(), left.getType(), right.getType()),
                    BOOLEAN,
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Context context)
        {
            List<RowExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            List<TypeSignatureProvider> argumentTypes = arguments.stream()
                    .map(RowExpression::getType)
                    .map(Type::getTypeSignature)
                    .map(TypeSignatureProvider::new)
                    .collect(toImmutableList());

            return call(node.getName().toString(),
                    functionAndTypeResolver.resolveFunction(
                            Optional.of(sessionFunctions),
                            transactionId,
                            functionAndTypeResolver.qualifyObjectName(node.getName()),
                            argumentTypes),
                    getType(node),
                    arguments);
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Context context)
        {
            VariableReferenceExpression variable = new VariableReferenceExpression(getSourceLocation(node), node.getName(), getType(node));
            Integer channel = layout.get(variable);
            if (channel != null) {
                return field(variable.getSourceLocation(), channel, variable.getType());
            }

            return variable;
        }

        @Override
        protected RowExpression visitLambdaExpression(LambdaExpression node, Context context)
        {
            RowExpression body = process(node.getBody(), context);

            Type type = getType(node);
            List<Type> typeParameters = type.getTypeParameters();
            List<Type> argumentTypes = typeParameters.subList(0, typeParameters.size() - 1);
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            return new LambdaDefinitionExpression(getSourceLocation(node), argumentTypes, argumentNames, body);
        }

        @Override
        protected RowExpression visitBindExpression(BindExpression node, Context context)
        {
            ImmutableList.Builder<Type> valueTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> argumentsBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                RowExpression valueRowExpression = process(value, context);
                valueTypesBuilder.add(valueRowExpression.getType());
                argumentsBuilder.add(valueRowExpression);
            }
            RowExpression function = process(node.getFunction(), context);
            argumentsBuilder.add(function);

            return specialForm(BIND, getType(node), argumentsBuilder.build());
        }

        @Override
        protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Context context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    getSourceLocation(node),
                    node.getOperator().name(),
                    functionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
                    getType(node),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Context context)
        {
            RowExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            getSourceLocation(node),
                            NEGATION.name(),
                            functionAndTypeResolver.resolveOperator(NEGATION, fromTypes(expression.getType())),
                            getType(node),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context)
        {
            Form form;
            switch (node.getOperator()) {
                case AND:
                    form = AND;
                    break;
                case OR:
                    form = OR;
                    break;
                default:
                    throw new IllegalStateException("Unknown logical operator: " + node.getOperator());
            }
            return specialForm(getSourceLocation(node), form, BOOLEAN, process(node.getLeft(), context), process(node.getRight(), context));
        }

        @Override
        protected RowExpression visitCast(Cast node, Context context)
        {
            RowExpression value = process(node.getExpression(), context);

            if (node.isSafe()) {
                return call(getSourceLocation(node), TRY_CAST.name(), functionAndTypeResolver.lookupCast("TRY_CAST", value.getType(), getType(node)), getType(node), value);
            }

            return call(getSourceLocation(node), CAST.name(), functionAndTypeResolver.lookupCast("CAST", value.getType(), getType(node)), getType(node), value);
        }

        @Override
        protected RowExpression visitCoalesceExpression(CoalesceExpression node, Context context)
        {
            List<RowExpression> arguments = node.getOperands().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            return specialForm(COALESCE, getType(node), arguments);
        }

        @Override
        protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Context context)
        {
            return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), getType(node), context);
        }

        @Override
        protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Context context)
        {
            // We rewrite this as - CASE true WHEN p1 THEN v1 WHEN p2 THEN v2 .. ELSE v END
            return buildSwitch(new ConstantExpression(getSourceLocation(node), true, BOOLEAN), node.getWhenClauses(), node.getDefaultValue(), getType(node), context);
        }

        private RowExpression buildSwitch(RowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Type returnType, Context context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(operand);

            for (WhenClause clause : whenClauses) {
                arguments.add(specialForm(
                        getSourceLocation(clause),
                        WHEN,
                        getType(clause.getResult()),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            arguments.add(defaultValue
                    .map((value) -> process(value, context))
                    .orElseGet(() -> constantNull(operand.getSourceLocation(), returnType)));

            return specialForm(SWITCH, returnType, arguments.build());
        }

        @Override
        protected RowExpression visitDereferenceExpression(DereferenceExpression node, Context context)
        {
            Type returnType = getType(node);
            Type baseType = getType(node.getBase());

            if (baseType == null) {
                return new UnresolvedSymbolExpression(getSourceLocation(node), returnType, getQualifiedName(node).getParts());
            }

            if (isEnumType(baseType) && isEnumType(returnType)) {
                return constant(resolveEnumLiteral(node, baseType), returnType);
            }

            if (baseType instanceof TypeWithName) {
                baseType = ((TypeWithName) baseType).getType();
            }

            if (baseType instanceof DistinctType) {
                baseType = ((DistinctType) baseType).getBaseType();
            }
            RowType rowType = (RowType) baseType;
            String fieldName = node.getField().getValue();
            List<Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            if (sqlFunctionProperties.isLegacyRowFieldOrdinalAccessEnabled() && index < 0) {
                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, fields);
                if (rowIndex.isPresent()) {
                    index = rowIndex.getAsInt();
                }
            }

            checkState(index >= 0, "could not find field name: %s", node.getField());
            return specialForm(getSourceLocation(node.getBase()), DEREFERENCE, returnType, process(node.getBase(), context), constant((long) index, INTEGER));
        }

        @Override
        protected RowExpression visitIfExpression(IfExpression node, Context context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getCondition(), context))
                    .add(process(node.getTrueValue(), context));

            if (node.getFalseValue().isPresent()) {
                arguments.add(process(node.getFalseValue().get(), context));
            }
            else {
                arguments.add(constantNull(getSourceLocation(node), getType(node)));
            }

            return specialForm(IF, getType(node), arguments.build());
        }

        @Override
        protected RowExpression visitTryExpression(TryExpression node, Context context)
        {
            RowExpression body = process(node.getInnerExpression(), context);

            return call(
                    functionAndTypeResolver,
                    "$internal$try",
                    getType(node),
                    new LambdaDefinitionExpression(
                            getSourceLocation(node),
                            ImmutableList.of(),
                            ImmutableList.of(),
                            body));
        }

        private RowExpression buildEquals(RowExpression lhs, RowExpression rhs)
        {
            return call(
                    EQUAL.getOperator(),
                    functionResolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, lhs.getType(), rhs.getType()),
                    BOOLEAN,
                    lhs,
                    rhs);
        }

        @Override
        protected RowExpression visitExists(ExistsPredicate existsPredicate, Context context)
        {
            RowExpression subquery = process(existsPredicate.getSubquery(), context);
            return new ExistsExpression(subquery.getSourceLocation(), subquery);
        }

        @Override
        protected RowExpression visitQuantifiedComparisonExpression(com.facebook.presto.sql.tree.QuantifiedComparisonExpression expression, Context context)
        {
            return quantifiedComparison(
                    OperatorType.valueOf(expression.getOperator().name()),
                    QuantifiedComparisonExpression.Quantifier.valueOf(expression.getQuantifier().name()),
                    process(expression.getValue(), context),
                    process(expression.getSubquery(), context));
        }

        @Override
        protected RowExpression visitInPredicate(InPredicate node, Context context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            RowExpression value = process(node.getValue(), context);
            if (!(node.getValueList() instanceof InListExpression)) {
                RowExpression subquery = process(node.getValueList(), context);
                checkArgument(value instanceof VariableReferenceExpression, "Unexpected expression: %s", value);
                checkArgument(subquery instanceof VariableReferenceExpression, "Unexpected expression: %s", subquery);
                return inSubquery((VariableReferenceExpression) value, (VariableReferenceExpression) subquery);
            }
            InListExpression values = (InListExpression) node.getValueList();

            if (values.getValues().size() == 1) {
                return buildEquals(value, process(values.getValues().get(0), context));
            }

            arguments.add(value);
            for (Expression inValue : values.getValues()) {
                arguments.add(process(inValue, context));
            }

            return specialForm(IN, BOOLEAN, arguments.build());
        }

        @Override
        protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Context context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(
                    getSourceLocation(node),
                    "not",
                    functionResolution.notFunction(),
                    BOOLEAN,
                    specialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected RowExpression visitIsNullPredicate(IsNullPredicate node, Context context)
        {
            RowExpression expression = process(node.getValue(), context);

            return specialForm(getSourceLocation(node), IS_NULL, BOOLEAN, expression);
        }

        @Override
        protected RowExpression visitNotExpression(NotExpression node, Context context)
        {
            return call(getSourceLocation(node), "not", functionResolution.notFunction(), BOOLEAN, process(node.getValue(), context));
        }

        @Override
        protected RowExpression visitNullIfExpression(NullIfExpression node, Context context)
        {
            RowExpression first = process(node.getFirst(), context);
            RowExpression second = process(node.getSecond(), context);
            Type returnType = getType(node);

            if (!functionAndTypeManager.nullIfSpecialFormEnabled()) {
                // If the first type is unknown, as per presto's NULL_IF semantics we should not infer the type using second argument.
                // Always return a null with unknown type.
                if (first.getType().equals(UnknownType.UNKNOWN)) {
                    return constantNull(UnknownType.UNKNOWN);
                }
                RowExpression firstArgWithoutCast = first;

                if (!second.getType().equals(first.getType())) {
                    Optional<Type> commonType = functionAndTypeResolver.getCommonSuperType(first.getType(), second.getType());
                    if (!commonType.isPresent()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", first.getType(), second.getType());
                    }

                    // cast(first as <common type>)
                    if (!first.getType().equals(commonType.get())) {
                        first = call(
                                getSourceLocation(node),
                                CAST.name(),
                                functionAndTypeResolver.lookupCast(CAST.name(), first.getType(), commonType.get()),
                                commonType.get(), first);
                    }
                    // cast(second as <common type>)
                    if (!second.getType().equals(commonType.get())) {
                        second = call(
                                getSourceLocation(node),
                                CAST.name(),
                                functionAndTypeResolver.lookupCast(CAST.name(), second.getType(), commonType.get()),
                                commonType.get(), second);
                    }
                }
                FunctionHandle equalsFunctionHandle = functionAndTypeResolver.resolveOperator(EQUAL, fromTypes(first.getType(), second.getType()));
                // equal(cast(first as <common type>), cast(second as <common type>))
                RowExpression equal = call(EQUAL.name(), equalsFunctionHandle, BOOLEAN, first, second);

                // if (equal(cast(first as <common type>), cast(second as <common type>)), cast(null as firstType), first)
                return specialForm(IF, returnType, equal, constantNull(returnType), firstArgWithoutCast);
            }
            return specialForm(getSourceLocation(node), NULL_IF, returnType, first, second);
        }

        @Override
        protected RowExpression visitBetweenPredicate(BetweenPredicate node, Context context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression min = process(node.getMin(), context);
            RowExpression max = process(node.getMax(), context);

            return call(
                    getSourceLocation(node),
                    BETWEEN.name(),
                    functionAndTypeResolver.resolveOperator(BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
                    BOOLEAN,
                    value,
                    min,
                    max);
        }

        @Override
        protected RowExpression visitLikePredicate(LikePredicate node, Context context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression pattern = process(node.getPattern(), context);

            if (node.getEscape().isPresent()) {
                RowExpression escape = process(node.getEscape().get(), context);
                if (!functionResolution.supportsLikePatternFunction()) {
                    return call(value.getSourceLocation(), "LIKE", functionResolution.likeVarcharVarcharVarcharFunction(), BOOLEAN, value, pattern, escape);
                }
                return likeFunctionCall(value, call(getSourceLocation(node), "LIKE_PATTERN", functionResolution.likePatternFunction(), LIKE_PATTERN, pattern, escape));
            }

            RowExpression prefixOrSuffixMatch = generateLikePrefixOrSuffixMatch(value, pattern);
            if (prefixOrSuffixMatch != null) {
                return prefixOrSuffixMatch;
            }

            if (!functionResolution.supportsLikePatternFunction()) {
                return likeFunctionCall(value, pattern);
            }

            return likeFunctionCall(value, call(getSourceLocation(node), CAST.name(), functionAndTypeResolver.lookupCast("CAST", VARCHAR, LIKE_PATTERN), LIKE_PATTERN, pattern));
        }

        private RowExpression generateLikePrefixOrSuffixMatch(RowExpression value, RowExpression pattern)
        {
            if (value.getType() instanceof VarcharType && pattern instanceof ConstantExpression) {
                Object constObject = ((ConstantExpression) pattern).getValue();
                if (constObject instanceof Slice) {
                    Slice slice = (Slice) constObject;
                    String patternString = slice.toStringUtf8();
                    int matchCharacterLength = patternString.length();
                    int matchBytesLength = slice.length();
                    if (matchCharacterLength > 1 && !patternString.contains("_")) {
                        if (LIKE_PREFIX_MATCH_PATTERN.matcher(patternString).matches()) {
                            // prefix match
                            // x LIKE 'some string%' is same as SUBSTR(x, 1, length('some string')) = 'some string', trialing .* won't matter
                            return buildEquals(
                                    call(functionAndTypeManager, "SUBSTR", VARCHAR, value, constant(1L, BIGINT), constant((long) matchCharacterLength - 1, BIGINT)),
                                    constant(slice.slice(0, matchBytesLength - 1), VARCHAR));
                        }
                        else if (LIKE_SUFFIX_MATCH_PATTERN.matcher(patternString).matches()) {
                            // suffix match
                            // x LIKE '%some string' is same as SUBSTR(x, 'some string', -length('some string')) = 'some stirng'
                            return buildEquals(
                                    call(functionAndTypeManager, "SUBSTR", VARCHAR, value, constant(-(long) (matchCharacterLength - 1), BIGINT)),
                                    constant(slice.slice(1, matchBytesLength - 1), VARCHAR));
                        }
                        else if (LIKE_SIMPLE_EXISTS_PATTERN.matcher(patternString).matches()) {
                            // pattern should just exist in the string ignoring leading and trailing stuff
                            // x LIKE '%some string%' is same as CARDINALITY(SPLIT(x, 'some string', 2)) = 2
                            // Split is most efficient as it uses string.indexOf java builtin so little memory/cpu overhead
                            return buildEquals(
                                    call(functionAndTypeManager, "CARDINALITY", BIGINT, call(functionAndTypeManager, "SPLIT", new ArrayType(VARCHAR), value, constant(slice.slice(1, matchBytesLength - 2), VARCHAR), constant(2L, BIGINT))),
                                    constant(2L, BIGINT));
                        }
                    }
                }
            }

            return null;
        }

        private RowExpression likeFunctionCall(RowExpression value, RowExpression pattern)
        {
            if (value.getType() instanceof VarcharType) {
                if (!functionResolution.supportsLikePatternFunction()) {
                    return call(value.getSourceLocation(), "LIKE", functionResolution.likeVarcharVarcharFunction(), BOOLEAN, value, pattern);
                }
                return call(value.getSourceLocation(), "LIKE", functionResolution.likeVarcharFunction(), BOOLEAN, value, pattern);
            }

            checkState(value.getType() instanceof CharType, "LIKE value type is neither VARCHAR or CHAR");
            return call(value.getSourceLocation(), "LIKE", functionResolution.likeCharFunction(value.getType()), BOOLEAN, value, pattern);
        }

        @Override
        protected RowExpression visitSubscriptExpression(SubscriptExpression node, Context context)
        {
            RowExpression base = process(node.getBase(), context);
            RowExpression index = process(node.getIndex(), context);

            // this block will handle row subscript, converts the ROW_CONSTRUCTOR with subscript to a DEREFERENCE expression
            if (base.getType() instanceof RowType) {
                checkState(index instanceof ConstantExpression, "Subscript expression on ROW requires a ConstantExpression");
                ConstantExpression position = (ConstantExpression) index;
                checkState(position.getValue() instanceof Long, "ConstantExpression should contain a valid integer index into the row");
                Long offset = (Long) position.getValue();
                checkState(
                        offset >= 1 && offset <= base.getType().getTypeParameters().size(),
                        "Subscript index out of bounds %s: should be >= 1 and <= %s",
                        offset,
                        base.getType().getTypeParameters().size());
                return specialForm(getSourceLocation(node), DEREFERENCE, getType(node), base, Expressions.constant(offset - 1, INTEGER));
            }
            return call(
                    getSourceLocation(node),
                    SUBSCRIPT.name(),
                    functionAndTypeResolver.resolveOperator(SUBSCRIPT, fromTypes(base.getType(), index.getType())),
                    getType(node),
                    base,
                    index);
        }

        @Override
        protected RowExpression visitArrayConstructor(ArrayConstructor node, Context context)
        {
            List<RowExpression> arguments = node.getValues().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            List<Type> argumentTypes = arguments.stream()
                    .map(RowExpression::getType)
                    .collect(toImmutableList());
            return call("ARRAY", functionResolution.arrayConstructor(argumentTypes), getType(node), arguments);
        }

        @Override
        protected RowExpression visitRow(Row node, Context context)
        {
            List<RowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = getType(node);
            return specialForm(ROW_CONSTRUCTOR, returnType, arguments);
        }

        @Override
        protected RowExpression visitCurrentTime(CurrentTime node, Context context)
        {
            if (node.getPrecision() != null) {
                throw new UnsupportedOperationException("not yet implemented: non-default precision");
            }

            switch (node.getFunction()) {
                case DATE:
                    return call(functionAndTypeResolver, "current_date", getType(node));
                case TIME:
                    return call(functionAndTypeResolver, "current_time", getType(node));
                case LOCALTIME:
                    return call(functionAndTypeResolver, "localtime", getType(node));
                case TIMESTAMP:
                    return call(functionAndTypeResolver, "current_timestamp", getType(node));
                case LOCALTIMESTAMP:
                    return call(functionAndTypeResolver, "localtimestamp", getType(node));
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getFunction());
            }
        }

        @Override
        protected RowExpression visitExtract(Extract node, Context context)
        {
            RowExpression value = process(node.getExpression(), context);
            switch (node.getField()) {
                case YEAR:
                    return call(functionAndTypeResolver, "year", getType(node), value);
                case QUARTER:
                    return call(functionAndTypeResolver, "quarter", getType(node), value);
                case MONTH:
                    return call(functionAndTypeResolver, "month", getType(node), value);
                case WEEK:
                    return call(functionAndTypeResolver, "week", getType(node), value);
                case DAY:
                case DAY_OF_MONTH:
                    return call(functionAndTypeResolver, "day", getType(node), value);
                case DAY_OF_WEEK:
                case DOW:
                    return call(functionAndTypeResolver, "day_of_week", getType(node), value);
                case DAY_OF_YEAR:
                case DOY:
                    return call(functionAndTypeResolver, "day_of_year", getType(node), value);
                case YEAR_OF_WEEK:
                case YOW:
                    return call(functionAndTypeResolver, "year_of_week", getType(node), value);
                case HOUR:
                    return call(functionAndTypeResolver, "hour", getType(node), value);
                case MINUTE:
                    return call(functionAndTypeResolver, "minute", getType(node), value);
                case SECOND:
                    return call(functionAndTypeResolver, "second", getType(node), value);
                case TIMEZONE_MINUTE:
                    return call(functionAndTypeResolver, "timezone_minute", getType(node), value);
                case TIMEZONE_HOUR:
                    return call(functionAndTypeResolver, "timezone_hour", getType(node), value);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node.getField());
        }

        @Override
        protected RowExpression visitAtTimeZone(AtTimeZone node, Context context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression timeZone = process(node.getTimeZone(), context);
            Type valueType = value.getType();
            if (valueType.equals(TIME)) {
                value = call(
                        getSourceLocation(node),
                        CAST.name(),
                        functionAndTypeResolver.lookupCast("CAST", valueType, TIME_WITH_TIME_ZONE),
                        TIME_WITH_TIME_ZONE,
                        value);
            }
            else if (valueType.equals(TIMESTAMP)) {
                value = call(
                        getSourceLocation(node),
                        CAST.name(),
                        functionAndTypeResolver.lookupCast("CAST", valueType, TIMESTAMP_WITH_TIME_ZONE),
                        TIMESTAMP_WITH_TIME_ZONE,
                        value);
            }

            return call(functionAndTypeResolver, "at_timezone", getType(node), value, timeZone);
        }

        @Override
        protected RowExpression visitCurrentUser(CurrentUser node, Context context)
        {
            return call(functionAndTypeResolver, "$current_user", getType(node));
        }
    }
}
