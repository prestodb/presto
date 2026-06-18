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
package com.facebook.presto.druid;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import static java.util.Objects.requireNonNull;

public abstract class DruidAggregationColumnNode
{
    private final ExpressionType expressionType;
    private final VariableReferenceExpression outputColumn;

    public DruidAggregationColumnNode(ExpressionType expressionType, VariableReferenceExpression outputColumn)
    {
        this.expressionType = requireNonNull(expressionType, "expressionType is null");
        this.outputColumn = requireNonNull(outputColumn, "outputColumn is null");
    }

    public VariableReferenceExpression getOutputColumn()
    {
        return outputColumn;
    }

    public ExpressionType getExpressionType()
    {
        return expressionType;
    }

    public enum ExpressionType
    {
        GROUP_BY,
        AGGREGATE,
    }

    public static class GroupByColumnNode
            extends DruidAggregationColumnNode
    {
        private final VariableReferenceExpression inputColumn;

        public GroupByColumnNode(VariableReferenceExpression inputColumn, VariableReferenceExpression output)
        {
            super(ExpressionType.GROUP_BY, output);
            this.inputColumn = requireNonNull(inputColumn, "inputColumn is null");
        }

        public VariableReferenceExpression getInputColumn()
        {
            return inputColumn;
        }

        @Override
        public String toString()
        {
            return inputColumn.toString();
        }
    }

    public static class AggregationFunctionColumnNode
            extends DruidAggregationColumnNode
    {
        private final CallExpression callExpression;

        public AggregationFunctionColumnNode(VariableReferenceExpression output, CallExpression callExpression)
        {
            super(ExpressionType.AGGREGATE, output);
            this.callExpression = requireNonNull(callExpression, "callExpression is null");
        }

        public CallExpression getCallExpression()
        {
            return callExpression;
        }

        @Override
        public String toString()
        {
            return callExpression.toString();
        }
    }
}
