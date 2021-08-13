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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import static java.util.Objects.requireNonNull;

public class ExternalCallExpressionChecker
        implements RowExpressionVisitor<Boolean, Void>
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public ExternalCallExpressionChecker(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public Boolean visitCall(CallExpression call, Void context)
    {
        FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
        if (functionMetadata.getImplementationType().isExternal()) {
            return true;
        }
        return call.getArguments().stream().anyMatch(argument -> argument.accept(this, null));
    }

    @Override
    public Boolean visitInputReference(InputReferenceExpression reference, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitConstant(ConstantExpression literal, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        // check in lambda is done in ExpressionAnalyzer so we should never reach here if we have external functions
        return false;
    }

    @Override
    public Boolean visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        return false;
    }

    @Override
    public Boolean visitSpecialForm(SpecialFormExpression specialForm, Void context)
    {
        return specialForm.getArguments().stream().anyMatch(argument -> argument.accept(this, null));
    }
}
