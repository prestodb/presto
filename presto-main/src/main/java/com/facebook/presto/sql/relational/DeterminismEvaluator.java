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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.relation.column.CallExpression;
import com.facebook.presto.spi.relation.column.ColumnExpression;
import com.facebook.presto.spi.relation.column.ColumnExpressionVisitor;
import com.facebook.presto.spi.relation.column.ColumnReferenceExpression;
import com.facebook.presto.spi.relation.column.ConstantExpression;
import com.facebook.presto.spi.relation.column.InputReferenceExpression;
import com.facebook.presto.spi.relation.column.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.column.VariableReferenceExpression;

import static java.util.Objects.requireNonNull;

public class DeterminismEvaluator
{
    final FunctionRegistry registry;

    public DeterminismEvaluator(FunctionRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
    }

    public boolean isDeterministic(ColumnExpression expression)
    {
        return expression.accept(new Visitor(registry), null);
    }

    private static class Visitor
            implements ColumnExpressionVisitor<Boolean, Void>
    {
        private final FunctionRegistry registry;

        public Visitor(FunctionRegistry registry)
        {
            this.registry = registry;
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitConstant(ConstantExpression literal, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitCall(CallExpression call, Void context)
        {
            Signature signature = call.getSignature();
            if (registry.isRegistered(signature) && !registry.getScalarFunctionImplementation(signature).isDeterministic()) {
                return false;
            }

            return call.getArguments().stream()
                    .allMatch(expression -> expression.accept(this, context));
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda.getBody().accept(this, context);
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitColumnReference(ColumnReferenceExpression columnReferenceExpression, Void context)
        {
            return true;
        }
    }
}
