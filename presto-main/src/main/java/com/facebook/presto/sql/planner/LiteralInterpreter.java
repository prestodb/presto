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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorInfo;
import com.facebook.presto.metadata.OperatorInfo.OperatorType;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.type.ColorType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTime;
import static com.facebook.presto.util.DateTimeUtils.parseTimestamp;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.facebook.presto.util.DateTimeUtils.printDate;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.Slices.utf8Slice;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, Session session, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(metadata).process(node, session);
    }

    public static List<Expression> toExpressions(List<?> objects, List<? extends Type> types)
    {
        checkNotNull(objects, "objects is null");
        checkNotNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(object, type));
        }
        return expressions.build();
    }

    public static Expression toExpression(Object object, Type type)
    {
        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object == null) {
            return new Cast(new NullLiteral(), type.getName());
        }

        if (type.equals(BIGINT)) {
            return new LongLiteral(object.toString());
        }

        if (type.equals(DOUBLE)) {
            Double value = (Double) object;
            if (value.isNaN()) {
                return new FunctionCall(new QualifiedName("nan"), ImmutableList.<Expression>of());
            }
            else if (value == Double.NEGATIVE_INFINITY) {
                return new NegativeExpression(new FunctionCall(new QualifiedName("infinity"), ImmutableList.<Expression>of()));
            }
            else if (value == Double.POSITIVE_INFINITY) {
                return new FunctionCall(new QualifiedName("infinity"), ImmutableList.<Expression>of());
            }
            else {
                return new DoubleLiteral(object.toString());
            }
        }

        if (type.equals(VARCHAR)) {
            if (object instanceof Slice) {
                return new StringLiteral(((Slice) object).toString(UTF_8));
            }

            if (object instanceof String) {
                return new StringLiteral((String) object);
            }
        }

        if (type.equals(BOOLEAN)) {
            return new BooleanLiteral(object.toString());
        }

        if (type.equals(DateType.DATE)) {
            return new GenericLiteral("DATE", printDate((Long) object));
        }

        if (type.equals(TimeType.TIME)) {
            return new FunctionCall(new QualifiedName("__to_time__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE)) {
            return new FunctionCall(new QualifiedName("__to_time_with_time_zone__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(TimestampType.TIMESTAMP)) {
            return new FunctionCall(new QualifiedName("__to_timestamp__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
            return new FunctionCall(new QualifiedName("__to_timestamp_with_time_zone__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(IntervalDayTimeType.INTERVAL_DAY_TIME)) {
            return new FunctionCall(new QualifiedName("__to_interval_day_time__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(IntervalYearMonthType.INTERVAL_YEAR_MONTH)) {
            return new FunctionCall(new QualifiedName("__to_interval_year_month__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        if (type.equals(ColorType.COLOR)) {
            return new FunctionCall(new QualifiedName("__to_color__"), ImmutableList.<Expression>of(new LongLiteral(object.toString())));
        }

        throw new UnsupportedOperationException("not yet implemented: " + object.getClass().getName());
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, Session>
    {
        private final Metadata metadata;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected Object visitLiteral(Literal node, Session session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Session session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, Session session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, Session session)
        {
            return node.getValue();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, Session session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, Session session)
        {
            Type type = metadata.getType(node.getType());
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            OperatorInfo operator;
            try {
                operator = metadata.getExactOperator(OperatorType.CAST, type, ImmutableList.of(VARCHAR));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
            }
            try {
                return ExpressionInterpreter.invoke(session, operator.getMethodHandle(), ImmutableList.<Object>of(utf8Slice(node.getValue())));
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
            }
        }

        @Override
        protected Long visitTimeLiteral(TimeLiteral node, Session session)
        {
            return parseTime(session.getTimeZoneKey(), node.getValue());
        }

        @Override
        protected Long visitTimestampLiteral(TimestampLiteral node, Session session)
        {
            return parseTimestamp(session.getTimeZoneKey(), node.getValue());
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, Session session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }

        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, Session session)
        {
            return null;
        }
    }
}
