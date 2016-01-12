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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.relational.Signatures.TRY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;

public class TryExpressionExtractor
        implements RowExpressionVisitor<Scope, BytecodeNode>
{
    private final ImmutableList.Builder<CallExpression> tryExpressions = ImmutableList.builder();

    @Override
    public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
    {
        // TODO: change such that CallExpressions only capture the inputs they actually depend on
        return null;
    }

    @Override
    public BytecodeNode visitCall(CallExpression call, Scope scope)
    {
        if (call.getSignature().getName().equals(TRY)) {
            checkState(call.getArguments().size() == 1, "try call expressions must have a single argument");
            checkState(getOnlyElement(call.getArguments()) instanceof CallExpression, "try call expression argument must be a call expression");

            tryExpressions.add((CallExpression) getOnlyElement(call.getArguments()));
        }

        for (RowExpression rowExpression : call.getArguments()) {
            rowExpression.accept(this, null);
        }

        return null;
    }

    @Override
    public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
    {
        return null;
    }

    public List<CallExpression> getTryExpressionsPreOrder()
    {
        return tryExpressions.build().reverse();
    }
}
