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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DateLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor().process(node, null);
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

        throw new UnsupportedOperationException("not yet implemented: " + object.getClass().getName());
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, Void>
    {
        @Override
        protected Object visitLiteral(Literal node, Void context)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, Void context)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitDateLiteral(DateLiteral node, Void context)
        {
            return node.getUnixTime();
        }

        @Override
        protected Object visitTimeLiteral(TimeLiteral node, Void context)
        {
            return node.getUnixTime();
        }

        @Override
        protected Long visitTimestampLiteral(TimestampLiteral node, Void context)
        {
            return node.getUnixTime();
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            if (node.isYearToMonth()) {
                throw new UnsupportedOperationException("Month based intervals not supported yet: " + node.getType());
            }
            return node.getSeconds();
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, Void context)
        {
            return null;
        }
    }
}
