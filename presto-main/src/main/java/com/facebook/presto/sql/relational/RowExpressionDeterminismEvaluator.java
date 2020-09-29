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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import javax.inject.Inject;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static java.util.Objects.requireNonNull;

public class RowExpressionDeterminismEvaluator
        implements DeterminismEvaluator
{
    private final FunctionAndTypeManager functionAndTypeManager;

    @Inject
    public RowExpressionDeterminismEvaluator(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null").getFunctionAndTypeManager());
    }

    public RowExpressionDeterminismEvaluator(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public boolean isDeterministic(RowExpression expression)
    {
        return expression.accept(new Visitor(functionAndTypeManager), null);
    }

    private static class Visitor
            implements RowExpressionVisitor<Boolean, Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        public Visitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
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
            FunctionHandle functionHandle = call.getFunctionHandle();
            try {
                if (!functionAndTypeManager.getFunctionMetadata(functionHandle).isDeterministic()) {
                    return false;
                }
            }
            catch (PrestoException e) {
                if (e.getErrorCode().getCode() != FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                    throw e;
                }
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
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return specialForm.getArguments().stream()
                    .allMatch(expression -> expression.accept(this, context));
        }
    }
}
