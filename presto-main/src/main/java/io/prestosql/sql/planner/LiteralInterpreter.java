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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static io.prestosql.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.DateTimeUtils.parseDayTimeInterval;
import static io.prestosql.util.DateTimeUtils.parseTimeLiteral;
import static io.prestosql.util.DateTimeUtils.parseTimestampLiteral;
import static io.prestosql.util.DateTimeUtils.parseYearMonthInterval;

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

    private static class LiteralVisitor
            extends AstVisitor<Object, ConnectorSession>
    {
        private final Metadata metadata;
        private final InterpretedFunctionInvoker functionInvoker;

        private LiteralVisitor(Metadata metadata)
        {
            this.metadata = metadata;
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionRegistry());
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
                Signature operatorSignature = new Signature("json_parse", SCALAR, JSON.getTypeSignature(), VARCHAR.getTypeSignature());
                return functionInvoker.invoke(operatorSignature, session, ImmutableList.of(utf8Slice(node.getValue())));
            }

            try {
                Signature signature = metadata.getFunctionRegistry().getCoercion(VARCHAR, type);
                return functionInvoker.invoke(signature, session, ImmutableList.of(utf8Slice(node.getValue())));
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
