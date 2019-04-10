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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class RowExpressionUtils
{
    private RowExpressionUtils()
    {
    }

    public static RowExpression inlineExpressions(RowExpression rowExpression, Map<RowExpression, RowExpression> assignments)
    {
        return rowExpression.accept(new InlineVisitor(), assignments).orElse(rowExpression);
    }

    private static class InlineVisitor
            implements RowExpressionVisitor<Optional<RowExpression>, Map<RowExpression, RowExpression>>
    {
        @Override
        public Optional<RowExpression> visitCall(CallExpression call, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(call)) {
                return Optional.of(context.get(call));
            }
            List<RowExpression> rewritten = returnChanged(call.getArguments(), argument -> argument.accept(this, context));
            if (rewritten.size() == call.getArguments().size()) {
                return Optional.of(new CallExpression(call.getDisplayName(), call.getFunctionHandle(), call.getType(), rewritten));
            }
            return Optional.empty();
        }

        @Override
        public Optional<RowExpression> visitInputReference(InputReferenceExpression reference, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(reference)) {
                return Optional.of(context.get(reference));
            }
            return Optional.empty();
        }

        @Override
        public Optional<RowExpression> visitConstant(ConstantExpression literal, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(literal)) {
                return Optional.of(context.get(literal));
            }
            return Optional.empty();
        }

        @Override
        public Optional<RowExpression> visitLambda(LambdaDefinitionExpression lambda, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(lambda)) {
                return Optional.of(context.get(lambda));
            }
            return Optional.empty();
        }

        @Override
        public Optional<RowExpression> visitVariableReference(VariableReferenceExpression reference, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(reference)) {
                return Optional.of(context.get(reference));
            }
            return Optional.empty();
        }

        @Override
        public Optional<RowExpression> visitSpecialForm(SpecialFormExpression specialForm, Map<RowExpression, RowExpression> context)
        {
            if (context.containsKey(specialForm)) {
                return Optional.of(context.get(specialForm));
            }
            List<RowExpression> rewritten = returnChanged(specialForm.getArguments(), argument -> argument.accept(this, context));
            if (rewritten.size() == specialForm.getArguments().size()) {
                return Optional.of(new SpecialFormExpression(specialForm.getForm(), specialForm.getType(), rewritten));
            }
            return Optional.empty();
        }

        private static <T> List<T> returnChanged(List<T> input, Function<T, Optional<T>> transformer)
        {
            Iterator<T> iterator = input.iterator();
            ImmutableList.Builder<T> builder = ImmutableList.builderWithExpectedSize(input.size());
            boolean changed = false;
            while (iterator.hasNext()) {
                T item = iterator.next();
                Optional<T> result = transformer.apply(item);
                builder.add(result.orElse(item));
                if (result.isPresent()) {
                    changed = true;
                }
            }
            if (changed) {
                return builder.build();
            }
            return ImmutableList.of();
        }
    }
}
