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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

public class GetVariablesFromRowExpressionVisitor
        implements RowExpressionVisitor<Set<VariableReferenceExpression>, Void>
{
    Set<VariableReferenceExpression> refs = new HashSet<>();

    @Override
    public Set<VariableReferenceExpression> visitExpression(RowExpression expression, Void context)
    {
        for (RowExpression child : expression.getChildren()) {
            child.accept(this, context);
        }
        return ImmutableSet.copyOf(refs);
    }

    @Override
    public Set<VariableReferenceExpression> visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        refs.add(reference);
        return ImmutableSet.copyOf(refs);
    }

    @Override
    public Set<VariableReferenceExpression> visitConstant(ConstantExpression literal, Void context)
    {
        return ImmutableSet.copyOf(refs);
    }
}
