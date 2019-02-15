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
package com.facebook.presto.operator.project;

import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnExpressionVisitor;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.ConstantExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.relational.Expressions.field;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Rewrite input references from columns in the input page (to the filter/project node)
 * into a compact list that can be used for method parameters.
 */
public final class PageFieldsToInputParametersRewriter
{
    private PageFieldsToInputParametersRewriter() {}

    public static Result rewritePageFieldsToInputParameters(ColumnExpression expression)
    {
        Visitor visitor = new Visitor();
        ColumnExpression rewrittenProjection = expression.accept(visitor, null);
        InputChannels inputChannels = new InputChannels(visitor.getInputChannels());
        return new Result(rewrittenProjection, inputChannels);
    }

    private static class Visitor
            implements ColumnExpressionVisitor<ColumnExpression, Void>
    {
        private final Map<Integer, Integer> fieldToParameter = new HashMap<>();
        private final List<Integer> inputChannels = new ArrayList<>();
        private int nextParameter;

        public List<Integer> getInputChannels()
        {
            return ImmutableList.copyOf(inputChannels);
        }

        @Override
        public ColumnExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            int parameter = getParameterForField(reference);
            return field(parameter, reference.getType());
        }

        private Integer getParameterForField(InputReferenceExpression reference)
        {
            return fieldToParameter.computeIfAbsent(reference.getField(), field -> {
                inputChannels.add(field);
                return nextParameter++;
            });
        }

        @Override
        public ColumnExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(
                    call.getSignature(),
                    call.getType(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public ColumnExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public ColumnExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(
                    lambda.getArgumentTypes(),
                    lambda.getArguments(),
                    lambda.getBody().accept(this, context));
        }

        @Override
        public ColumnExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public ColumnExpression visitColumnReference(ColumnReferenceExpression reference, Void context)
        {
            return reference;
        }
    }

    public static class Result
    {
        private final ColumnExpression rewrittenExpression;
        private final InputChannels inputChannels;

        public Result(ColumnExpression rewrittenExpression, InputChannels inputChannels)
        {
            this.rewrittenExpression = rewrittenExpression;
            this.inputChannels = inputChannels;
        }

        public ColumnExpression getRewrittenExpression()
        {
            return rewrittenExpression;
        }

        public InputChannels getInputChannels()
        {
            return inputChannels;
        }
    }
}
