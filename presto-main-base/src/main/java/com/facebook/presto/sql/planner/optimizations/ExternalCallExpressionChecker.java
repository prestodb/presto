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
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ExternalCallExpressionChecker
        implements RowExpressionVisitor<Boolean, Void>
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Set<String> rpcFunctionNames;

    public ExternalCallExpressionChecker(FunctionAndTypeManager functionAndTypeManager)
    {
        this(functionAndTypeManager, ImmutableSet.of());
    }

    /**
     * @param rpcFunctionNames lower-case display names of functions a worker serves over RPC. A
     * caller that treats such a call as remote passes them here; one that only cares about the
     * function's declared implementation type uses the other constructor. A checker visits every
     * call in an expression, so a caller that checks many expressions fetches the names once and
     * passes the set.
     */
    public ExternalCallExpressionChecker(FunctionAndTypeManager functionAndTypeManager, Set<String> rpcFunctionNames)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.rpcFunctionNames = ImmutableSet.copyOf(requireNonNull(rpcFunctionNames, "rpcFunctionNames is null"));
    }

    @Override
    public Boolean visitCall(CallExpression call, Void context)
    {
        FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
        if (functionMetadata.getImplementationType().isExternalExecution()) {
            return true;
        }
        // A function dispatched over RPC also leaves the worker's own address space, but its
        // implementation type is the one the worker registered it under, which
        // isExternalExecution() does not cover. The set is supplied rather than derived because
        // only the worker knows which of its functions it dispatches that way.
        if (rpcFunctionNames.contains(call.getDisplayName().toLowerCase(ENGLISH))) {
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
        return lambda.getBody().accept(this, null);
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
