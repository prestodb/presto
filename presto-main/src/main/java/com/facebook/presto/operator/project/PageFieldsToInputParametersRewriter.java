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

import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.SpecialFormExpression;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
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

    public static Result rewritePageFieldsToInputParameters(RowExpression expression)
    {
        Visitor visitor = new Visitor();
        RowExpression rewrittenProjection = expression.accept(visitor, null);
        InputChannels inputChannels = new InputChannels(visitor.getInputChannels());
        return new Result(rewrittenProjection, inputChannels);
    }

    private static class Visitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<Integer, Integer> fieldToParameter = new HashMap<>();
        private final List<Integer> inputChannels = new ArrayList<>();
        private int nextParameter;

        public List<Integer> getInputChannels()
        {
            return ImmutableList.copyOf(inputChannels);
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
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
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(
                    lambda.getArgumentTypes(),
                    lambda.getArguments(),
                    lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream()
                            .map(expression -> expression.accept(this, context))
                            .collect(toImmutableList()));
        }
    }

    public static class Result
    {
        private final RowExpression rewrittenExpression;
        private final InputChannels inputChannels;

        public Result(RowExpression rewrittenExpression, InputChannels inputChannels)
        {
            this.rewrittenExpression = rewrittenExpression;
            this.inputChannels = inputChannels;
        }

        public RowExpression getRewrittenExpression()
        {
            return rewrittenExpression;
        }

        public InputChannels getInputChannels()
        {
            return inputChannels;
        }
    }
}
