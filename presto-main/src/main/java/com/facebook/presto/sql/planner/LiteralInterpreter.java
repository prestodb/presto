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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.type.IntervalDayTimeType;
import com.facebook.presto.type.IntervalYearMonthType;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.facebook.presto.type.SqlIntervalYearMonth;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.DateTimeUtils.parseDayTimeInterval;
import static com.facebook.presto.util.DateTimeUtils.parseTimeLiteral;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.parseYearMonthInterval;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public final class LiteralInterpreter
{
    private LiteralInterpreter() {}

    public static Object evaluate(Metadata metadata, ConnectorSession session, Expression node)
    {
        if (!(node instanceof Literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }
        return new LiteralVisitor(metadata).process(node, session);
    }

    public static Object evaluate(ConnectorSession session, ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            return null;
        }
        if (type instanceof BooleanType) {
            return node.getValue();
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            return node.getValue();
        }
        if (type instanceof DoubleType) {
            return node.getValue();
        }
        if (type instanceof RealType) {
            Long number = (Long) node.getValue();
            return intBitsToFloat(number.intValue());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(node.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) node.getValue()), decimalType);
            }
            checkState(node.getValue() instanceof Slice);
            Slice value = (Slice) node.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return ((Slice) node.getValue()).toStringUtf8();
        }
        if (type instanceof VarbinaryType) {
            return new SqlVarbinary(((Slice) node.getValue()).getBytes());
        }
        if (type instanceof DateType) {
            return new SqlDate(((Long) node.getValue()).intValue());
        }
        if (type instanceof TimeType) {
            if (session.isLegacyTimestamp()) {
                return new SqlTime((long) node.getValue(), session.getTimeZoneKey());
            }
            return new SqlTime((long) node.getValue());
        }
        if (type instanceof TimestampType) {
            try {
                if (session.isLegacyTimestamp()) {
                    return new SqlTimestamp((long) node.getValue(), session.getTimeZoneKey());
                }
                return new SqlTimestamp((long) node.getValue());
            }
            catch (RuntimeException e) {
                throw new PrestoException(GENERIC_USER_ERROR, format("'%s' is not a valid timestamp literal", (String) node.getValue()));
            }
        }
        if (type instanceof IntervalDayTimeType) {
            return new SqlIntervalDayTime((long) node.getValue());
        }
        if (type instanceof IntervalYearMonthType) {
            return new SqlIntervalYearMonth(((Long) node.getValue()).intValue());
        }

        // We should not fail at the moment; just return the raw value (block, regex, etc) to the user
        return node.getValue();
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;
        private final InterpretedFunctionInvoker functionInvoker;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionManager());
        }

        @Override
        protected Object visitLiteral(Literal node, ConnectorSession session)
        {
            throw new UnsupportedOperationException("Unhandled literal type: " + node);
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Long visitLongLiteral(LongLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Double visitDoubleLiteral(DoubleLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitDecimalLiteral(DecimalLiteral node, ConnectorSession context)
        {
            return Decimals.parse(node.getValue()).getObject();
        }

        @Override
        protected Slice visitStringLiteral(StringLiteral node, ConnectorSession session)
        {
            return node.getSlice();
        }

        @Override
        protected Object visitCharLiteral(CharLiteral node, ConnectorSession context)
        {
            return node.getSlice();
        }

        @Override
        protected Slice visitBinaryLiteral(BinaryLiteral node, ConnectorSession session)
        {
            return node.getValue();
        }

        @Override
        protected Object visitGenericLiteral(GenericLiteral node, ConnectorSession session)
        {
            Type type = metadata.getType(parseTypeSignature(node.getType()));
            if (type == null) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (JSON.equals(type)) {
                FunctionHandle functionHandle = metadata.getFunctionManager().lookupFunction("json_parse", fromTypes(VARCHAR));
                return functionInvoker.invoke(functionHandle, session, ImmutableList.of(utf8Slice(node.getValue())));
            }

            try {
                FunctionHandle functionHandle = metadata.getFunctionManager().lookupCast(CAST, VARCHAR.getTypeSignature(), type.getTypeSignature());
                return functionInvoker.invoke(functionHandle, session, ImmutableList.of(utf8Slice(node.getValue())));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
            }
        }

        @Override
        protected Long visitTimeLiteral(TimeLiteral node, ConnectorSession session)
        {
            if (session.isLegacyTimestamp()) {
                return parseTimeLiteral(session.getTimeZoneKey(), node.getValue());
            }
            else {
                return parseTimeLiteral(node.getValue());
            }
        }

        @Override
        protected Long visitTimestampLiteral(TimestampLiteral node, ConnectorSession session)
        {
            try {
                if (session.isLegacyTimestamp()) {
                    return parseTimestampLiteral(session.getTimeZoneKey(), node.getValue());
                }
                else {
                    return parseTimestampLiteral(node.getValue());
                }
            }
            catch (RuntimeException e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid timestamp literal", node.getValue());
            }
        }

        @Override
        protected Long visitIntervalLiteral(IntervalLiteral node, ConnectorSession session)
        {
            if (node.isYearToMonth()) {
                return node.getSign().multiplier() * parseYearMonthInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
            else {
                return node.getSign().multiplier() * parseDayTimeInterval(node.getValue(), node.getStartField(), node.getEndField());
            }
        }

        @Override
        protected Object visitNullLiteral(NullLiteral node, ConnectorSession session)
        {
            return null;
        }
    }
}
