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

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.filterKeys;

public final class VariableToChannelTranslator
{
    private VariableToChannelTranslator() {}

    /**
     * Given an {@param expression} and a {@param layout}, translate the symbols in the expression to the corresponding channel.
     */
    public static RowExpression translate(RowExpression expression, Map<VariableReferenceExpression, Integer> layout)
    {
        return expression.accept(new Visitor(), layout);
    }

    private static class Visitor
            implements RowExpressionVisitor<RowExpression, Map<VariableReferenceExpression, Integer>>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression input, Map<VariableReferenceExpression, Integer> layout)
        {
            throw new UnsupportedOperationException("encountered already-translated symbols");
        }

        @Override
        public RowExpression visitCall(CallExpression call, Map<VariableReferenceExpression, Integer> layout)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            call.getArguments().forEach(argument -> arguments.add(argument.accept(this, layout)));
            return call(call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments.build());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, Integer> layout)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, Integer> layout)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, layout));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Map<VariableReferenceExpression, Integer> layout)
        {
            // We only use the variable name to find the reference in layout because SqlToRowExpression translator might optimize type cast
            // to a variable with the same name as in layout but with a different type.
            // TODO https://github.com/prestodb/presto/issues/12892
            Map<VariableReferenceExpression, Integer> candidate = filterKeys(layout, variable -> variable.getName().equals(reference.getName()));
            if (!candidate.isEmpty()) {
                return field(getOnlyElement(candidate.values()), reference.getType());
            }
            // this is possible only for lambda
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, Integer> layout)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            specialForm.getArguments().forEach(argument -> arguments.add(argument.accept(this, layout)));
            return new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), arguments.build());
        }
    }
}
