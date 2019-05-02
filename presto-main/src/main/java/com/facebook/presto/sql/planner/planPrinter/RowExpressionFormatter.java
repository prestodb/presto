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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.LiteralInterpreter;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class RowExpressionFormatter
{
    private final ConnectorSession session;

    public RowExpressionFormatter(ConnectorSession session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    public String formatRowExpression(RowExpression expression)
    {
        return expression.accept(new Formatter(), null);
    }

    public List<String> formatRowExpressions(List<RowExpression> rowExpressions)
    {
        return rowExpressions.stream().map(this::formatRowExpression).collect(toList());
    }

    public class Formatter
            implements RowExpressionVisitor<String, Void>
    {
        @Override
        public String visitCall(CallExpression node, Void context)
        {
            return node.getFunctionHandle().getSignature().getName() + "(" + String.join(", ", formatRowExpressions(node.getArguments())) + ")";
        }

        @Override
        public String visitSpecialForm(SpecialFormExpression node, Void context)
        {
            return node.getForm().name() + "(" + String.join(", ", formatRowExpressions(node.getArguments())) + ")";
        }

        @Override
        public String visitInputReference(InputReferenceExpression node, Void context)
        {
            return node.toString();
        }

        @Override
        public String visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return "(" + String.join(", ", node.getArguments()) + ") -> " + formatRowExpression(node.getBody());
        }

        @Override
        public String visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return node.getName();
        }

        @Override
        public String visitConstant(ConstantExpression node, Void context)
        {
            Object value = LiteralInterpreter.evaluate(session, node);

            if (value == null) {
                return String.valueOf((Object) null);
            }

            Type type = node.getType();
            if (node.getType().getJavaType() == Block.class) {
                Block block = (Block) value;
                // TODO: format block
                return format("[Block: position count: %s; size: %s bytes]", block.getPositionCount(), block.getRetainedSizeInBytes());
            }
            return type.getDisplayName().toUpperCase() + " " + value.toString();
        }
    }
}
