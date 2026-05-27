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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.Streams;

import java.util.Optional;

import static java.lang.String.format;

public class RowExpressionEquivalenceVisitor
        implements RowExpressionVisitor<Boolean, RowExpression>
{
    @Override
    public Boolean visitExpression(RowExpression expression, RowExpression context)
    {
        // All other types of expressions are not yet supported. This is work in progress.
        throw new RuntimeException(format("An expression of unknown type detected: %s", expression));
    }

    @Override
    public Boolean visitConstant(ConstantExpression literal, RowExpression context)
    {
        if (!(context instanceof ConstantExpression) || !compare(context.getType(), literal.getType())) {
            return false;
        }
        return ((ConstantExpression) context).getValue().equals(literal.getValue());
    }

    @Override
    public Boolean visitVariableReference(VariableReferenceExpression reference, RowExpression context)
    {
        if (!(context instanceof VariableReferenceExpression) || !compare(context.getType(), reference.getType())) {
            return false;
        }
        return ((VariableReferenceExpression) context).getName().equals(reference.getName());
    }

    @Override
    public Boolean visitCall(CallExpression leftCallExpression, RowExpression rightRowExpression)
    {
        if (!(rightRowExpression instanceof CallExpression)) {
            return false;
        }
        CallExpression rightCallExpression = (CallExpression) rightRowExpression;
        if (rightCallExpression.getArguments().size() != leftCallExpression.getArguments().size() ||
                (!rightCallExpression.getFunctionHandle().canonicalize().equals(leftCallExpression.getFunctionHandle().canonicalize())) ||
                (!rightCallExpression.getDisplayName().equals(leftCallExpression.getDisplayName())) ||
                (!compare(rightCallExpression.getType(), leftCallExpression.getType()))) {
            return false;
        }
        // Next let's check if arguments are equal.
        Optional<Boolean> reduce = Streams.zip(leftCallExpression.getArguments().stream(), rightCallExpression.getArguments().stream(),
                (leftRexp, rightRexp) -> {
                    if (leftRexp != null && rightRexp != null) {
                        return leftRexp.accept(this, rightRexp);
                    }
                    return false;
                }).reduce((left, right) -> left && Boolean.TRUE.equals(right));
        return reduce.orElse(false);
    }

    @Override
    public Boolean visitSpecialForm(SpecialFormExpression leftSpecialFormExpression, RowExpression rightRowExpression)
    {
        if (!(rightRowExpression instanceof SpecialFormExpression)) {
            return false;
        }
        SpecialFormExpression rightSpecialFormExpression = (SpecialFormExpression) rightRowExpression;
        if (rightSpecialFormExpression.getArguments().size() != leftSpecialFormExpression.getArguments().size() ||
                (!rightSpecialFormExpression.getForm().equals(leftSpecialFormExpression.getForm())) ||
                (!compare(rightSpecialFormExpression.getType(), leftSpecialFormExpression.getType()))) {
            return false;
        }
        // Next let's check if arguments are equal.
        Optional<Boolean> reduce = Streams.zip(leftSpecialFormExpression.getArguments().stream(), rightSpecialFormExpression.getArguments().stream(),
                (leftRexp, rightRexp) -> {
                    if (leftRexp != null && rightRexp != null) {
                        return leftRexp.accept(this, rightRexp);
                    }
                    return false;
                }).reduce((left, right) -> left && Boolean.TRUE.equals(right));
        return reduce.orElse(false);
    }

    private static Boolean compare(Type left, Type right)
    { // We compare only base type for types with params. because, varchar(1) != varchar and can make two instances of same function different.
        return left.getTypeSignature().getBase().equals(right.getTypeSignature().getBase());
    }
}
