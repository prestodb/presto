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
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UnknownType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.Expression;
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
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

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
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.TRY_CAST;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
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
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.tryResolveEnumLiteral;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
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
                Optional.of(session.getUser()),
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                session.getSessionFunctions());
    }

    public static RowExpression translate(
            Expression expression,
            Map<NodeRef<Expression>, Type> types,
            Map<VariableReferenceExpression, Integer> layout,
            FunctionAndTypeManager functionAndTypeManager,
            Optional<String> user,
            Optional<TransactionId> transactionId,
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions)
    {
        Visitor visitor = new Visitor(
                types,
                layout,
                functionAndTypeManager,
                user,
                transactionId,
                sqlFunctionProperties,
                sessionFunctions);
        RowExpression result = visitor.process(expression, null);
        requireNonNull(result, "translated expression is null");
        return result;
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;
        private final Map<VariableReferenceExpression, Integer> layout;
        private final FunctionAndTypeManager functionAndTypeManager;
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
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.layout = layout;
            this.functionAndTypeManager = functionAndTypeManager;
            this.user = user;
            this.transactionId = transactionId;
            this.sqlFunctionProperties = sqlFunctionProperties;
            this.functionResolution = new FunctionResolution(functionAndTypeManager);
            this.sessionFunctions = sessionFunctions;
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        protected RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitIdentifier(Identifier node, Void context)
        {
            // identifier should never be reachable with the exception of lambda within VALUES (#9711)
            return new VariableReferenceExpression(node.getValue(), getType(node));
        }

        @Override
        protected RowExpression visitCurrentUser(CurrentUser node, Void context)
        {
            return user.map(user -> constant(Slices.utf8Slice(user), VARCHAR)).orElseThrow(() -> new UnsupportedOperationException("Do not have current user"));
        }

        @Override
        protected RowExpression visitFieldReference(FieldReference node, Void context)
        {
            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        protected RowExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected RowExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected RowExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected RowExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
        }

        @Override
        protected RowExpression visitCharLiteral(CharLiteral node, Void context)
        {
            return constant(node.getSlice(), createCharType(node.getValue().length()));
        }

        @Override
        protected RowExpression visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return constant(node.getValue(), VARBINARY);
        }

        @Override
        protected RowExpression visitEnumLiteral(EnumLiteral node, Void context)
        {
            Type type;
            try {
                type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            return constant(node.getValue(), type);
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type;
            try {
                type = functionAndTypeManager.getType(parseTypeSignature(node.getType()));
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
                else if (BIGINT.equals(type)) {
                    return constant(Long.parseLong(node.getValue()), BIGINT);
                }
            }
            catch (NumberFormatException e) {
                throw new SemanticException(SemanticErrorCode.INVALID_LITERAL, node, format("Invalid formatted generic %s literal: %s", type, node));
            }

            if (JSON.equals(type)) {
                return call(
                        "json_parse",
                        functionAndTypeManager.lookupFunction("json_parse", fromTypes(VARCHAR)),
                        getType(node),
                        constant(utf8Slice(node.getValue()), VARCHAR));
            }

            return call(
                    CAST.name(),
                    functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), getType(node).getTypeSignature()),
                    getType(node),
                    constant(utf8Slice(node.getValue()), VARCHAR));
        }

        @Override
        protected RowExpression visitTimeLiteral(TimeLiteral node, Void context)
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
        protected RowExpression visitTimestampLiteral(TimestampLiteral node, Void context)
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
        protected RowExpression visitIntervalLiteral(IntervalLiteral node, Void context)
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
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.comparisonFunction(node.getOperator(), left.getType(), right.getType()),
                    BOOLEAN,
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Void context)
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
                    functionAndTypeManager.resolveFunction(
                            Optional.of(sessionFunctions),
                            transactionId,
                            qualifyObjectName(node.getName()),
                            argumentTypes),
                    getType(node),
                    arguments);
        }

        @Override
        protected RowExpression visitSymbolReference(SymbolReference node, Void context)
        {
            VariableReferenceExpression variable = new VariableReferenceExpression(node.getName(), getType(node));
            Integer channel = layout.get(variable);
            if (channel != null) {
                return field(channel, variable.getType());
            }

            return variable;
        }

        @Override
        protected RowExpression visitLambdaExpression(LambdaExpression node, Void context)
        {
            RowExpression body = process(node.getBody(), context);

            Type type = getType(node);
            List<Type> typeParameters = type.getTypeParameters();
            List<Type> argumentTypes = typeParameters.subList(0, typeParameters.size() - 1);
            List<String> argumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            return new LambdaDefinitionExpression(argumentTypes, argumentNames, body);
        }

        @Override
        protected RowExpression visitBindExpression(BindExpression node, Void context)
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
        protected RowExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    node.getOperator().name(),
                    functionResolution.arithmeticFunction(node.getOperator(), left.getType(), right.getType()),
                    getType(node),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            NEGATION.name(),
                            functionAndTypeManager.resolveOperator(NEGATION, fromTypes(expression.getType())),
                            getType(node),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
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
            return specialForm(form, BOOLEAN, process(node.getLeft(), context), process(node.getRight(), context));
        }

        @Override
        protected RowExpression visitCast(Cast node, Void context)
        {
            RowExpression value = process(node.getExpression(), context);

            if (node.isSafe()) {
                return call(TRY_CAST.name(), functionAndTypeManager.lookupCast(TRY_CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
            }

            return call(CAST.name(), functionAndTypeManager.lookupCast(CAST, value.getType().getTypeSignature(), getType(node).getTypeSignature()), getType(node), value);
        }

        @Override
        protected RowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<RowExpression> arguments = node.getOperands().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            return specialForm(COALESCE, getType(node), arguments);
        }

        @Override
        protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            return buildSwitch(process(node.getOperand(), context), node.getWhenClauses(), node.getDefaultValue(), getType(node), context);
        }

        @Override
        protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            // We rewrite this as - CASE true WHEN p1 THEN v1 WHEN p2 THEN v2 .. ELSE v END
            return buildSwitch(new ConstantExpression(true, BOOLEAN), node.getWhenClauses(), node.getDefaultValue(), getType(node), context);
        }

        private RowExpression buildSwitch(RowExpression operand, List<WhenClause> whenClauses, Optional<Expression> defaultValue, Type returnType, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(operand);

            for (WhenClause clause : whenClauses) {
                arguments.add(specialForm(
                        WHEN,
                        getType(clause.getResult()),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            arguments.add(defaultValue
                    .map((value) -> process(value, context))
                    .orElse(constantNull(returnType)));

            return specialForm(SWITCH, returnType, arguments.build());
        }

        @Override
        protected RowExpression visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            Type returnType = getType(node);
            Optional<Object> maybeEnumLiteral = tryResolveEnumLiteral(node, returnType);
            if (maybeEnumLiteral.isPresent()) {
                return constant(maybeEnumLiteral.get(), returnType);
            }

            RowType rowType = (RowType) getType(node.getBase());
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
            return specialForm(DEREFERENCE, returnType, process(node.getBase(), context), constant((long) index, INTEGER));
        }

        @Override
        protected RowExpression visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getCondition(), context))
                    .add(process(node.getTrueValue(), context));

            if (node.getFalseValue().isPresent()) {
                arguments.add(process(node.getFalseValue().get(), context));
            }
            else {
                arguments.add(constantNull(getType(node)));
            }

            return specialForm(IF, getType(node), arguments.build());
        }

        @Override
        protected RowExpression visitTryExpression(TryExpression node, Void context)
        {
            throw new UnsupportedOperationException("Must desugar TryExpression before translate it into RowExpression");
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
        protected RowExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            RowExpression value = process(node.getValue(), context);
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
        protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(
                    "not",
                    functionResolution.notFunction(),
                    BOOLEAN,
                    specialForm(IS_NULL, BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected RowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return specialForm(IS_NULL, BOOLEAN, expression);
        }

        @Override
        protected RowExpression visitNotExpression(NotExpression node, Void context)
        {
            return call("not", functionResolution.notFunction(), BOOLEAN, process(node.getValue(), context));
        }

        @Override
        protected RowExpression visitNullIfExpression(NullIfExpression node, Void context)
        {
            RowExpression first = process(node.getFirst(), context);
            RowExpression second = process(node.getSecond(), context);

            return specialForm(NULL_IF, getType(node), first, second);
        }

        @Override
        protected RowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression min = process(node.getMin(), context);
            RowExpression max = process(node.getMax(), context);

            return call(
                    BETWEEN.name(),
                    functionAndTypeManager.resolveOperator(BETWEEN, fromTypes(value.getType(), min.getType(), max.getType())),
                    BOOLEAN,
                    value,
                    min,
                    max);
        }

        @Override
        protected RowExpression visitLikePredicate(LikePredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression pattern = process(node.getPattern(), context);

            if (node.getEscape().isPresent()) {
                RowExpression escape = process(node.getEscape().get(), context);
                return likeFunctionCall(value, call("LIKE_PATTERN", functionResolution.likePatternFunction(), LIKE_PATTERN, pattern, escape));
            }

            return likeFunctionCall(value, call(CAST.name(), functionAndTypeManager.lookupCast(CAST, VARCHAR.getTypeSignature(), LIKE_PATTERN.getTypeSignature()), LIKE_PATTERN, pattern));
        }

        private RowExpression likeFunctionCall(RowExpression value, RowExpression pattern)
        {
            if (value.getType() instanceof VarcharType) {
                return call("LIKE", functionResolution.likeVarcharFunction(), BOOLEAN, value, pattern);
            }

            checkState(value.getType() instanceof CharType, "LIKE value type is neither VARCHAR or CHAR");
            return call("LIKE", functionResolution.likeCharFunction(value.getType()), BOOLEAN, value, pattern);
        }

        @Override
        protected RowExpression visitSubscriptExpression(SubscriptExpression node, Void context)
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
                return specialForm(DEREFERENCE, getType(node), base, Expressions.constant(offset - 1, INTEGER));
            }
            return call(
                    SUBSCRIPT.name(),
                    functionAndTypeManager.resolveOperator(SUBSCRIPT, fromTypes(base.getType(), index.getType())),
                    getType(node),
                    base,
                    index);
        }

        @Override
        protected RowExpression visitArrayConstructor(ArrayConstructor node, Void context)
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
        protected RowExpression visitRow(Row node, Void context)
        {
            List<RowExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = getType(node);
            return specialForm(ROW_CONSTRUCTOR, returnType, arguments);
        }
    }
}
