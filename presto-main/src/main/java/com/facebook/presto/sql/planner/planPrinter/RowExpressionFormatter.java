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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.relational.FunctionResolution;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class RowExpressionFormatter
{
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;

    public RowExpressionFormatter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionMetadataManager = requireNonNull(functionAndTypeManager, "function manager is null");
        this.standardFunctionResolution = new FunctionResolution(functionAndTypeManager);
    }

    public String formatRowExpression(ConnectorSession session, RowExpression expression)
    {
        return expression.accept(new Formatter(), requireNonNull(session, "session is null"));
    }

    private List<String> formatRowExpressions(ConnectorSession session, List<RowExpression> rowExpressions)
    {
        return rowExpressions.stream().map(rowExpression -> formatRowExpression(session, rowExpression)).collect(toList());
    }

    public class Formatter
            implements RowExpressionVisitor<String, ConnectorSession>
    {
        @Override
        public String visitCall(CallExpression node, ConnectorSession session)
        {
            if (standardFunctionResolution.isArithmeticFunction(node.getFunctionHandle()) || standardFunctionResolution.isComparisonFunction(node.getFunctionHandle())) {
                String operation = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle()).getOperatorType().get().getOperator();
                return String.join(" " + operation + " ", formatRowExpressions(session, node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
            }
            else if (standardFunctionResolution.isCastFunction(node.getFunctionHandle())) {
                return String.format("CAST(%s AS %s)", formatRowExpression(session, node.getArguments().get(0)), node.getType().getDisplayName());
            }
            else if (standardFunctionResolution.isNegateFunction(node.getFunctionHandle())) {
                return "-(" + formatRowExpression(session, node.getArguments().get(0)) + ")";
            }
            else if (standardFunctionResolution.isSubscriptFunction(node.getFunctionHandle())) {
                return formatRowExpression(session, node.getArguments().get(0)) + "[" + formatRowExpression(session, node.getArguments().get(1)) + "]";
            }
            else if (standardFunctionResolution.isBetweenFunction(node.getFunctionHandle())) {
                List<String> formattedExpresions = formatRowExpressions(session, node.getArguments());
                return String.format("%s BETWEEN (%s) AND (%s)", formattedExpresions.get(0), formattedExpresions.get(1), formattedExpresions.get(2));
            }
            return node.getDisplayName() + "(" + String.join(", ", formatRowExpressions(session, node.getArguments())) + ")";
        }

        @Override
        public String visitSpecialForm(SpecialFormExpression node, ConnectorSession session)
        {
            if (node.getForm().equals(SpecialFormExpression.Form.AND) || node.getForm().equals(SpecialFormExpression.Form.OR)) {
                return String.join(" " + node.getForm() + " ", formatRowExpressions(session, node.getArguments()).stream().map(e -> "(" + e + ")").collect(toImmutableList()));
            }
            return node.getForm().name() + "(" + String.join(", ", formatRowExpressions(session, node.getArguments())) + ")";
        }

        @Override
        public String visitInputReference(InputReferenceExpression node, ConnectorSession session)
        {
            return node.toString();
        }

        @Override
        public String visitLambda(LambdaDefinitionExpression node, ConnectorSession session)
        {
            return "(" + String.join(", ", node.getArguments()) + ") -> " + formatRowExpression(session, node.getBody());
        }

        @Override
        public String visitVariableReference(VariableReferenceExpression node, ConnectorSession session)
        {
            return node.getName();
        }

        @Override
        public String visitConstant(ConstantExpression node, ConnectorSession session)
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
