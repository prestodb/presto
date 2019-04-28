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

import com.facebook.presto.spi.plan.Symbol;
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

public final class SymbolToChannelTranslator
{
    private SymbolToChannelTranslator() {}

    /**
     *
     * Given an {@param expression} and a {@param layout}, translate the symbols in the expression to the corresponding channel.
     */
    public static RowExpression translate(RowExpression expression, Map<Symbol, Integer> layout)
    {
        return expression.accept(new Visitor(), layout);
    }

    private static class Visitor
            implements RowExpressionVisitor<RowExpression, Map<Symbol, Integer>>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression input, Map<Symbol, Integer> layout)
        {
            throw new UnsupportedOperationException("encountered already-translated symbols");
        }

        @Override
        public RowExpression visitCall(CallExpression call, Map<Symbol, Integer> layout)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            call.getArguments().forEach(argument -> arguments.add(argument.accept(this, layout)));
            return call(call.getDisplayName(), call.getFunctionHandle(), call.getType(), arguments.build());
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Map<Symbol, Integer> layout)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Map<Symbol, Integer> layout)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, layout));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Map<Symbol, Integer> layout)
        {
            Symbol symbol = new Symbol(reference.getName());
            if (layout.containsKey(symbol)) {
                return field(layout.get(symbol), reference.getType());
            }
            // this is possible only for lambda
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Map<Symbol, Integer> layout)
        {
            ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
            specialForm.getArguments().forEach(argument -> arguments.add(argument.accept(this, layout)));
            return new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), arguments.build());
        }
    }
}
