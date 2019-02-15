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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnExpressionVisitor;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.ConstantExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalParseResult;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.RowType.Field;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
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
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
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
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isLegacyRowFieldOrdinalAccessEnabled;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Signatures.arithmeticExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.arithmeticNegationSignature;
import static com.facebook.presto.sql.relational.Signatures.arrayConstructorSignature;
import static com.facebook.presto.sql.relational.Signatures.betweenSignature;
import static com.facebook.presto.sql.relational.Signatures.bindSignature;
import static com.facebook.presto.sql.relational.Signatures.castSignature;
import static com.facebook.presto.sql.relational.Signatures.coalesceSignature;
import static com.facebook.presto.sql.relational.Signatures.comparisonExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.dereferenceSignature;
import static com.facebook.presto.sql.relational.Signatures.likeCharSignature;
import static com.facebook.presto.sql.relational.Signatures.likePatternSignature;
import static com.facebook.presto.sql.relational.Signatures.likeVarcharSignature;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.nullIfSignature;
import static com.facebook.presto.sql.relational.Signatures.rowConstructorSignature;
import static com.facebook.presto.sql.relational.Signatures.subscriptSignature;
import static com.facebook.presto.sql.relational.Signatures.switchSignature;
import static com.facebook.presto.sql.relational.Signatures.tryCastSignature;
import static com.facebook.presto.sql.relational.Signatures.whenSignature;
import static com.facebook.presto.type.JsonType.JSON;
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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class SqlToColumnExpressionTranslator
{
    private SqlToColumnExpressionTranslator() {}

    public static ColumnExpression translate(
            Expression expression,
            FunctionKind functionKind,
            Map<NodeRef<Expression>, Type> types,
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            boolean optimize)
    {
        return translate(expression, functionKind, types, ImmutableMap.of(), functionRegistry, typeManager, session, optimize);
    }

    public static ColumnExpression translate(
            Expression expression,
            FunctionKind functionKind,
            Map<NodeRef<Expression>, Type> types,
            Map<String, ColumnHandle> columnHandles,
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            boolean optimize)
    {
        return translate(expression, functionKind, types, columnHandles, ImmutableList.of(), functionRegistry, typeManager, session, optimize);
    }

    public static ColumnExpression translate(
            Expression expression,
            FunctionKind functionKind,
            Map<NodeRef<Expression>, Type> types,
            Map<String, ColumnHandle> columnHandles,
            List<String> inputChannelNames,
            FunctionRegistry functionRegistry,
            TypeManager typeManager,
            Session session,
            boolean optimize)
    {
        Visitor visitor = new Visitor(
                functionKind,
                types,
                columnHandles,
                inputChannelNames,
                typeManager,
                session.getTimeZoneKey(),
                isLegacyRowFieldOrdinalAccessEnabled(session),
                SystemSessionProperties.isLegacyTimestamp(session));
        ColumnExpression result = visitor.process(expression, null);

        requireNonNull(result, "translated expression is null");

        if (optimize) {
            ExpressionOptimizer optimizer = new ExpressionOptimizer(functionRegistry, typeManager, session);
            return optimizer.optimize(result);
        }

        return result;
    }

    private static class Visitor
            extends AstVisitor<ColumnExpression, Void>
    {
        private final FunctionKind functionKind;
        private final Map<NodeRef<Expression>, Type> types;
        private final TypeManager typeManager;
        private final TimeZoneKey timeZoneKey;
        private final boolean legacyRowFieldOrdinalAccess;
        @Deprecated
        private final boolean isLegacyTimestamp;
        private final Map<String, ColumnHandle> columnHandles;
        private final Map<String, Integer> inputChannels;

        private Visitor(
                FunctionKind functionKind,
                Map<NodeRef<Expression>, Type> types,
                Map<String, ColumnHandle> columnHandles,
                List<String> inputChannelNames,
                TypeManager typeManager,
                TimeZoneKey timeZoneKey,
                boolean legacyRowFieldOrdinalAccess,
                boolean isLegacyTimestamp)
        {
            requireNonNull(inputChannelNames, "inputChannelNames is null");
            this.functionKind = functionKind;
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
            this.columnHandles = ImmutableMap.copyOf(requireNonNull(columnHandles, "columnHandleMap is null"));
            this.inputChannels = IntStream.range(0, inputChannelNames.size())
                    .boxed()
                    .collect(toMap(i -> inputChannelNames.get(i), i -> i));
            this.typeManager = typeManager;
            this.timeZoneKey = timeZoneKey;
            this.legacyRowFieldOrdinalAccess = legacyRowFieldOrdinalAccess;
            this.isLegacyTimestamp = isLegacyTimestamp;
        }

        private Type getType(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        @Override
        protected ColumnExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected ColumnExpression visitFieldReference(FieldReference node, Void context)
        {
            return field(node.getFieldIndex(), getType(node));
        }

        @Override
        protected ColumnExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return constantNull(UnknownType.UNKNOWN);
        }

        @Override
        protected ColumnExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return constant(node.getValue(), BOOLEAN);
        }

        @Override
        protected ColumnExpression visitLongLiteral(LongLiteral node, Void context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return constant(node.getValue(), INTEGER);
            }
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected ColumnExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected ColumnExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return constant(parseResult.getObject(), parseResult.getType());
        }

        @Override
        protected ColumnExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(node.getSlice(), createVarcharType(countCodePoints(node.getSlice())));
        }

        @Override
        protected ColumnExpression visitCharLiteral(CharLiteral node, Void context)
        {
            return constant(node.getSlice(), createCharType(node.getValue().length()));
        }

        @Override
        protected ColumnExpression visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return constant(node.getValue(), VARBINARY);
        }

        @Override
        protected ColumnExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type;
            try {
                type = typeManager.getType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            if (JSON.equals(type)) {
                return call(
                        new Signature("json_parse", SCALAR, getType(node).getTypeSignature(), VARCHAR.getTypeSignature()),
                        getType(node),
                        constant(utf8Slice(node.getValue()), VARCHAR));
            }

            return call(
                    castSignature(getType(node), VARCHAR),
                    getType(node),
                    constant(utf8Slice(node.getValue()), VARCHAR));
        }

        @Override
        protected ColumnExpression visitTimeLiteral(TimeLiteral node, Void context)
        {
            long value;
            if (getType(node).equals(TIME_WITH_TIME_ZONE)) {
                value = parseTimeWithTimeZone(node.getValue());
            }
            else {
                if (isLegacyTimestamp) {
                    // parse in time zone of client
                    value = parseTimeWithoutTimeZone(timeZoneKey, node.getValue());
                }
                else {
                    value = parseTimeWithoutTimeZone(node.getValue());
                }
            }
            return constant(value, getType(node));
        }

        @Override
        protected ColumnExpression visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            long value;
            if (isLegacyTimestamp) {
                value = parseTimestampLiteral(timeZoneKey, node.getValue());
            }
            else {
                value = parseTimestampLiteral(node.getValue());
            }
            return constant(value, getType(node));
        }

        @Override
        protected ColumnExpression visitIntervalLiteral(IntervalLiteral node, Void context)
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
        protected ColumnExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ColumnExpression left = process(node.getLeft(), context);
            ColumnExpression right = process(node.getRight(), context);

            return call(
                    comparisonExpressionSignature(node.getOperator(), left.getType(), right.getType()),
                    BOOLEAN,
                    left,
                    right);
        }

        @Override
        protected ColumnExpression visitFunctionCall(FunctionCall node, Void context)
        {
            List<ColumnExpression> arguments = node.getArguments().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            List<TypeSignature> argumentTypes = arguments.stream()
                    .map(ColumnExpression::getType)
                    .map(Type::getTypeSignature)
                    .collect(toImmutableList());

            Signature signature = new Signature(node.getName().getSuffix(), functionKind, getType(node).getTypeSignature(), argumentTypes);

            return call(signature, getType(node), arguments);
        }

        @Override
        protected ColumnExpression visitSymbolReference(SymbolReference node, Void context)
        {
            if (columnHandles.containsKey(node.getName())) {
                return new ColumnReferenceExpression(columnHandles.get(node.getName()), getType(node));
            }
            else if (inputChannels.containsKey(node.getName())) {
                return new InputReferenceExpression(inputChannels.get(node.getName()), getType(node));
            }
            return new VariableReferenceExpression(node.getName(), getType(node));
        }

        @Override
        protected ColumnExpression visitLambdaExpression(LambdaExpression node, Void context)
        {
            ColumnExpression body = process(node.getBody(), context);

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
        protected ColumnExpression visitBindExpression(BindExpression node, Void context)
        {
            ImmutableList.Builder<Type> valueTypesBuilder = ImmutableList.builder();
            ImmutableList.Builder<ColumnExpression> argumentsBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                ColumnExpression valueColumnExpression = process(value, context);
                valueTypesBuilder.add(valueColumnExpression.getType());
                argumentsBuilder.add(valueColumnExpression);
            }
            ColumnExpression function = process(node.getFunction(), context);
            argumentsBuilder.add(function);

            return call(
                    bindSignature(getType(node), valueTypesBuilder.build(), function.getType()),
                    getType(node),
                    argumentsBuilder.build());
        }

        @Override
        protected ColumnExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            ColumnExpression left = process(node.getLeft(), context);
            ColumnExpression right = process(node.getRight(), context);

            return call(
                    arithmeticExpressionSignature(node.getOperator(), getType(node), left.getType(), right.getType()),
                    getType(node),
                    left,
                    right);
        }

        @Override
        protected ColumnExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            ColumnExpression expression = process(node.getValue(), context);

            switch (node.getSign()) {
                case PLUS:
                    return expression;
                case MINUS:
                    return call(
                            arithmeticNegationSignature(getType(node), expression.getType()),
                            getType(node),
                            expression);
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected ColumnExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return call(
                    logicalExpressionSignature(node.getOperator()),
                    BOOLEAN,
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected ColumnExpression visitCast(Cast node, Void context)
        {
            ColumnExpression value = process(node.getExpression(), context);

            if (node.isTypeOnly()) {
                return changeType(value, getType(node));
            }

            if (node.isSafe()) {
                return call(tryCastSignature(getType(node), value.getType()), getType(node), value);
            }

            return call(castSignature(getType(node), value.getType()), getType(node), value);
        }

        private static ColumnExpression changeType(ColumnExpression value, Type targetType)
        {
            ChangeTypeVisitor visitor = new ChangeTypeVisitor(targetType);
            return value.accept(visitor, null);
        }

        private static class ChangeTypeVisitor
                implements ColumnExpressionVisitor<ColumnExpression, Void>
        {
            private final Type targetType;

            private ChangeTypeVisitor(Type targetType)
            {
                this.targetType = targetType;
            }

            @Override
            public ColumnExpression visitCall(CallExpression call, Void context)
            {
                return new CallExpression(call.getSignature(), targetType, call.getArguments());
            }

            @Override
            public ColumnExpression visitInputReference(InputReferenceExpression reference, Void context)
            {
                return field(reference.getField(), targetType);
            }

            @Override
            public ColumnExpression visitConstant(ConstantExpression literal, Void context)
            {
                return constant(literal.getValue(), targetType);
            }

            @Override
            public ColumnExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnExpression visitVariableReference(VariableReferenceExpression reference, Void context)
            {
                return new VariableReferenceExpression(reference.getName(), targetType);
            }

            @Override
            public ColumnExpression visitColumnReference(ColumnReferenceExpression reference, Void context)
            {
                return new ColumnReferenceExpression(reference.getColumnHandle(), targetType);
            }
        }

        @Override
        protected ColumnExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<ColumnExpression> arguments = node.getOperands().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());

            List<Type> argumentTypes = arguments.stream().map(ColumnExpression::getType).collect(toImmutableList());
            return call(coalesceSignature(getType(node), argumentTypes), getType(node), arguments);
        }

        @Override
        protected ColumnExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<ColumnExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getOperand(), context));

            for (WhenClause clause : node.getWhenClauses()) {
                arguments.add(call(whenSignature(getType(clause)),
                        getType(clause),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            Type returnType = getType(node);

            arguments.add(node.getDefaultValue()
                    .map((value) -> process(value, context))
                    .orElse(constantNull(returnType)));

            return call(switchSignature(returnType), returnType, arguments.build());
        }

        @Override
        protected ColumnExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            /*
                Translates an expression like:

                    case when cond1 then value1
                         when cond2 then value2
                         when cond3 then value3
                         else value4
                    end

                To:

                    IF(cond1,
                        value1,
                        IF(cond2,
                            value2,
                                If(cond3,
                                    value3,
                                    value4)))

             */
            ColumnExpression expression = node.getDefaultValue()
                    .map((value) -> process(value, context))
                    .orElse(constantNull(getType(node)));

            for (WhenClause clause : Lists.reverse(node.getWhenClauses())) {
                expression = call(
                        Signatures.ifSignature(getType(node)),
                        getType(node),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context),
                        expression);
            }

            return expression;
        }

        @Override
        protected ColumnExpression visitDereferenceExpression(DereferenceExpression node, Void context)
        {
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

            if (legacyRowFieldOrdinalAccess && index < 0) {
                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, fields);
                if (rowIndex.isPresent()) {
                    index = rowIndex.getAsInt();
                }
            }

            checkState(index >= 0, "could not find field name: %s", node.getField());
            Type returnType = getType(node);
            return call(dereferenceSignature(returnType, rowType), returnType, process(node.getBase(), context), constant(index, INTEGER));
        }

        @Override
        protected ColumnExpression visitIfExpression(IfExpression node, Void context)
        {
            ImmutableList.Builder<ColumnExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getCondition(), context))
                    .add(process(node.getTrueValue(), context));

            if (node.getFalseValue().isPresent()) {
                arguments.add(process(node.getFalseValue().get(), context));
            }
            else {
                arguments.add(constantNull(getType(node)));
            }

            return call(Signatures.ifSignature(getType(node)), getType(node), arguments.build());
        }

        @Override
        protected ColumnExpression visitTryExpression(TryExpression node, Void context)
        {
            return call(Signatures.trySignature(getType(node)), getType(node), process(node.getInnerExpression(), context));
        }

        @Override
        protected ColumnExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<ColumnExpression> arguments = ImmutableList.builder();
            arguments.add(process(node.getValue(), context));
            InListExpression values = (InListExpression) node.getValueList();
            for (Expression value : values.getValues()) {
                arguments.add(process(value, context));
            }

            return call(Signatures.inSignature(), BOOLEAN, arguments.build());
        }

        @Override
        protected ColumnExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            ColumnExpression expression = process(node.getValue(), context);

            return call(
                    Signatures.notSignature(),
                    BOOLEAN,
                    call(Signatures.isNullSignature(expression.getType()), BOOLEAN, ImmutableList.of(expression)));
        }

        @Override
        protected ColumnExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            ColumnExpression expression = process(node.getValue(), context);

            return call(Signatures.isNullSignature(expression.getType()), BOOLEAN, expression);
        }

        @Override
        protected ColumnExpression visitNotExpression(NotExpression node, Void context)
        {
            return call(Signatures.notSignature(), BOOLEAN, process(node.getValue(), context));
        }

        @Override
        protected ColumnExpression visitNullIfExpression(NullIfExpression node, Void context)
        {
            ColumnExpression first = process(node.getFirst(), context);
            ColumnExpression second = process(node.getSecond(), context);

            return call(
                    nullIfSignature(getType(node), first.getType(), second.getType()),
                    getType(node),
                    first,
                    second);
        }

        @Override
        protected ColumnExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            ColumnExpression value = process(node.getValue(), context);
            ColumnExpression min = process(node.getMin(), context);
            ColumnExpression max = process(node.getMax(), context);

            return call(
                    betweenSignature(value.getType(), min.getType(), max.getType()),
                    BOOLEAN,
                    value,
                    min,
                    max);
        }

        @Override
        protected ColumnExpression visitLikePredicate(LikePredicate node, Void context)
        {
            ColumnExpression value = process(node.getValue(), context);
            ColumnExpression pattern = process(node.getPattern(), context);

            if (node.getEscape().isPresent()) {
                ColumnExpression escape = process(node.getEscape().get(), context);
                return likeFunctionCall(value, call(likePatternSignature(), LIKE_PATTERN, pattern, escape));
            }

            return likeFunctionCall(value, call(castSignature(LIKE_PATTERN, VARCHAR), LIKE_PATTERN, pattern));
        }

        private ColumnExpression likeFunctionCall(ColumnExpression value, ColumnExpression pattern)
        {
            if (value.getType() instanceof VarcharType) {
                return call(likeVarcharSignature(), BOOLEAN, value, pattern);
            }

            checkState(value.getType() instanceof CharType, "LIKE value type is neither VARCHAR or CHAR");
            return call(likeCharSignature(value.getType()), BOOLEAN, value, pattern);
        }

        @Override
        protected ColumnExpression visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            ColumnExpression base = process(node.getBase(), context);
            ColumnExpression index = process(node.getIndex(), context);

            return call(
                    subscriptSignature(getType(node), base.getType(), index.getType()),
                    getType(node),
                    base,
                    index);
        }

        @Override
        protected ColumnExpression visitArrayConstructor(ArrayConstructor node, Void context)
        {
            List<ColumnExpression> arguments = node.getValues().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            List<Type> argumentTypes = arguments.stream()
                    .map(ColumnExpression::getType)
                    .collect(toImmutableList());
            return call(arrayConstructorSignature(getType(node), argumentTypes), getType(node), arguments);
        }

        @Override
        protected ColumnExpression visitRow(Row node, Void context)
        {
            List<ColumnExpression> arguments = node.getItems().stream()
                    .map(value -> process(value, context))
                    .collect(toImmutableList());
            Type returnType = getType(node);
            List<Type> argumentTypes = node.getItems().stream()
                    .map(this::getType)
                    .collect(toImmutableList());
            return call(rowConstructorSignature(returnType, argumentTypes), returnType, arguments);
        }
    }

    private static class InlineInputChannelVistor
            implements ColumnExpressionVisitor<ColumnExpression, Void>
    {
        private final List<ColumnExpression> inputs;

        private InlineInputChannelVistor(List<ColumnExpression> inputs)
        {
            this.inputs = requireNonNull(inputs, "input is null");
        }

        public static ColumnExpression inlineInputs(ColumnExpression columnExpression, List<ColumnExpression> inputs)
        {
            return columnExpression.accept(new InlineInputChannelVistor(inputs), null);
        }

        @Override
        public ColumnExpression visitCall(CallExpression call, Void context)
        {
            return call.replaceChildren(call.getArguments().stream()
                    .map(argument -> argument.accept(this, context)).collect(toImmutableList()));
        }

        @Override
        public ColumnExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            int field = reference.getField();
            checkArgument(field >= 0 && field < inputs.size(), "Unknown input field");
            return inputs.get(field);
        }

        @Override
        public ColumnExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public ColumnExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            ColumnExpression children = lambda.getBody().accept(this, context);
            return lambda.replaceChildren(ImmutableList.of(children));
        }

        @Override
        public ColumnExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public ColumnExpression visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            return columnReferenceExpression;
        }
    }

    private static class ChangeColumnReferencetoInputReferenceVistor
            implements ColumnExpressionVisitor<ColumnExpression, Void>
    {
        private final List<ColumnReferenceExpression> inputColumns;

        private ChangeColumnReferencetoInputReferenceVistor(List<ColumnReferenceExpression> inputColumns)
        {
            this.inputColumns = inputColumns;
        }

        public static ColumnExpression rewriteColumnReferenceToInputReference(ColumnExpression columnExpression, List<ColumnReferenceExpression> inputColumns)
        {
            return columnExpression.accept(new ChangeColumnReferencetoInputReferenceVistor(inputColumns), null);
        }

        @Override
        public ColumnExpression visitCall(CallExpression call, Void context)
        {
            return call.replaceChildren(call.getArguments().stream()
                    .map(argument -> argument.accept(this, context)).collect(toImmutableList()));
        }

        @Override
        public ColumnExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public ColumnExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public ColumnExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            ColumnExpression children = lambda.getBody().accept(this, context);
            return lambda.replaceChildren(ImmutableList.of(children));
        }

        @Override
        public ColumnExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public ColumnExpression visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            int i = inputColumns.indexOf(columnReferenceExpression);
            checkArgument(i >= 0, "Column %s does not exists in input", columnReferenceExpression);
            return new InputReferenceExpression(i, columnReferenceExpression.getType());
        }
    }
}
