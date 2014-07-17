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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.optimizer.ExpressionOptimizer;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.UnknownType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.RowExpression.typeGetter;
import static com.facebook.presto.sql.relational.Signatures.arithmeticExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.arithmeticNegationSignature;
import static com.facebook.presto.sql.relational.Signatures.betweenSignature;
import static com.facebook.presto.sql.relational.Signatures.castSignature;
import static com.facebook.presto.sql.relational.Signatures.coalesceSignature;
import static com.facebook.presto.sql.relational.Signatures.comparisonExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.likePatternSignature;
import static com.facebook.presto.sql.relational.Signatures.likeSignature;
import static com.facebook.presto.sql.relational.Signatures.logicalExpressionSignature;
import static com.facebook.presto.sql.relational.Signatures.nullIfSignature;
import static com.facebook.presto.sql.relational.Signatures.switchSignature;
import static com.facebook.presto.sql.relational.Signatures.tryCastSignature;
import static com.facebook.presto.sql.relational.Signatures.whenSignature;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimeWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;

public final class SqlToRowExpressionTranslator
{
    private SqlToRowExpressionTranslator()
    {
    }

    public static RowExpression translate(Expression expression, IdentityHashMap<Expression, Type> types, Metadata metadata, ConnectorSession session, boolean optimize)
    {
        RowExpression result = new Visitor(types, metadata, session.getTimeZoneKey()).process(expression, null);

        Preconditions.checkNotNull(result, "translated expression is null");

        if (optimize) {
            ExpressionOptimizer optimizer = new ExpressionOptimizer(metadata.getFunctionRegistry(), session);
            return optimizer.optimize(result);
        }

        return result;
    }

    public static List<RowExpression> translate(List<Expression> expressions, IdentityHashMap<Expression, Type> types, Metadata metadata, ConnectorSession session, boolean optimize)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        for (Expression expression : expressions) {
            builder.add(translate(expression, types, metadata, session, optimize));
        }
        return builder.build();
    }

    private static class Visitor
            extends AstVisitor<RowExpression, Void>
    {
        private final IdentityHashMap<Expression, Type> types;
        private final Metadata metadata;
        private final TimeZoneKey timeZoneKey;

        private Visitor(IdentityHashMap<Expression, Type> types, Metadata metadata, TimeZoneKey timeZoneKey)
        {
            this.types = types;
            this.metadata = metadata;
            this.timeZoneKey = timeZoneKey;
        }

        @Override
        protected RowExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        @Override
        protected RowExpression visitInputReference(InputReference node, Void context)
        {
            return field(node.getChannel(), types.get(node));
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
            return constant(node.getValue(), BIGINT);
        }

        @Override
        protected RowExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return constant(node.getValue(), DOUBLE);
        }

        @Override
        protected RowExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return constant(node.getSlice(), VARCHAR);
        }

        @Override
        protected RowExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            Type type = metadata.getType(node.getType());
            if (type == null) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            return call(
                    castSignature(types.get(node), VARCHAR),
                    constant(Slices.copiedBuffer(node.getValue(), StandardCharsets.UTF_8), VARCHAR));
        }

        @Override
        protected RowExpression visitTimeLiteral(TimeLiteral node, Void context)
        {
            long value;
            if (types.get(node).equals(TIME_WITH_TIME_ZONE)) {
                value = parseTimeWithTimeZone(node.getValue());
            }
            else {
                // parse in time zone of client
                value = parseTimeWithoutTimeZone(timeZoneKey, node.getValue());
            }
            return constant(value, types.get(node));
        }

        @Override
        protected RowExpression visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            long value;
            if (types.get(node).equals(TIMESTAMP_WITH_TIME_ZONE)) {
                value = parseTimestampWithTimeZone(node.getValue());
            }
            else {
                // parse in time zone of client
                value = parseTimestampWithoutTimeZone(timeZoneKey, node.getValue());
            }
            return constant(value, types.get(node));
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
            return constant(value, types.get(node));
        }

        @Override
        protected RowExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    comparisonExpressionSignature(node.getType(), left.getType(), right.getType()),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitFunctionCall(FunctionCall node, Void context)
        {
            List<RowExpression> arguments = Lists.transform(node.getArguments(), processFunction(context));

            List<Type> argumentTypes = Lists.transform(arguments, typeGetter());
            Signature signature = new Signature(node.getName().getSuffix(), types.get(node), argumentTypes, false, false);

            return call(signature, arguments);
        }

        @Override
        protected RowExpression visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            RowExpression left = process(node.getLeft(), context);
            RowExpression right = process(node.getRight(), context);

            return call(
                    arithmeticExpressionSignature(node.getType(), types.get(node), left.getType(), right.getType()),
                    left,
                    right);
        }

        @Override
        protected RowExpression visitNegativeExpression(NegativeExpression node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(
                    arithmeticNegationSignature(types.get(node), expression.getType()),
                    expression);
        }

        @Override
        protected RowExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return call(
                    logicalExpressionSignature(node.getType()),
                    process(node.getLeft(), context),
                    process(node.getRight(), context));
        }

        @Override
        protected RowExpression visitCast(Cast node, Void context)
        {
            RowExpression value = process(node.getExpression(), context);

            if (node.isSafe()) {
                return call(tryCastSignature(types.get(node), value.getType()), value);
            }

            return call(castSignature(types.get(node), value.getType()), value);
        }

        @Override
        protected RowExpression visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            List<RowExpression> arguments = Lists.transform(node.getOperands(), processFunction(context));

            List<Type> argumentTypes = Lists.transform(arguments, typeGetter());
            return call(coalesceSignature(types.get(node), argumentTypes), arguments);
        }

        @Override
        protected RowExpression visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

            arguments.add(process(node.getOperand(), context));

            for (WhenClause clause : node.getWhenClauses()) {
                arguments.add(call(whenSignature(types.get(clause)),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context)));
            }

            Type returnType = types.get(node);
            if (node.getDefaultValue() != null) {
                arguments.add(process(node.getDefaultValue(), context));
            }
            else {
                arguments.add(constantNull(returnType));
            }

            return call(switchSignature(returnType), arguments.build());
        }

        @Override
        protected RowExpression visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
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
            RowExpression expression = constantNull(types.get(node));

            if (node.getDefaultValue() != null) {
                expression = process(node.getDefaultValue(), context);
            }

            for (WhenClause clause : Lists.reverse(node.getWhenClauses())) {
                expression = call(
                        Signatures.ifSignature(types.get(node)),
                        process(clause.getOperand(), context),
                        process(clause.getResult(), context),
                        expression);
            }

            return expression;
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
                arguments.add(constantNull(types.get(node)));
            }

            return call(Signatures.ifSignature(types.get(node)), arguments.build());
        }

        @Override
        protected RowExpression visitInPredicate(InPredicate node, Void context)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            arguments.add(process(node.getValue(), context));
            InListExpression values = (InListExpression) node.getValueList();
            for (Expression value : values.getValues()) {
                arguments.add(process(value, context));
            }

            return call(Signatures.inSignature(), arguments.build());
        }

        @Override
        protected RowExpression visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(
                    Signatures.notSignature(),
                    call(Signatures.isNullSignature(expression.getType()), ImmutableList.of(expression)));
        }

        @Override
        protected RowExpression visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            RowExpression expression = process(node.getValue(), context);

            return call(Signatures.isNullSignature(expression.getType()), expression);
        }

        @Override
        protected RowExpression visitNotExpression(NotExpression node, Void context)
        {
            return call(Signatures.notSignature(), process(node.getValue(), context));
        }

        @Override
        protected RowExpression visitNullIfExpression(NullIfExpression node, Void context)
        {
            RowExpression first = process(node.getFirst(), context);
            RowExpression second = process(node.getSecond(), context);

            return call(
                    nullIfSignature(types.get(node), first.getType(), second.getType()),
                    first,
                    second);
        }

        @Override
        protected RowExpression visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression min = process(node.getMin(), context);
            RowExpression max = process(node.getMax(), context);

            return call(
                    betweenSignature(value.getType(), min.getType(), max.getType()),
                    value,
                    min,
                    max);
        }

        @Override
        protected RowExpression visitLikePredicate(LikePredicate node, Void context)
        {
            RowExpression value = process(node.getValue(), context);
            RowExpression pattern = process(node.getPattern(), context);

            if (node.getEscape() != null) {
                RowExpression escape = process(node.getEscape(), context);
                return call(likeSignature(), value, call(likePatternSignature(), pattern, escape));
            }

            return call(likeSignature(), value, call(castSignature(LIKE_PATTERN, VARCHAR), pattern));
        }
    }
}
