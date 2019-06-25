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
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.specialForm;

public class RowExpressionTypeChanger
{
    private RowExpressionTypeChanger() {}

    public static RowExpression changeType(RowExpression value, Type targetType)
    {
        //TODO make it private in RowExpressionInterpreter once ExpressionOptimizer is deprecated.
        ChangeTypeVisitor visitor = new ChangeTypeVisitor(targetType);
        return value.accept(visitor, null);
    }

    private static class ChangeTypeVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Type targetType;

        private ChangeTypeVisitor(Type targetType)
        {
            this.targetType = targetType;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(call.getDisplayName(), call.getFunctionHandle(), targetType, call.getArguments());
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return field(reference.getField(), targetType);
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return constant(literal.getValue(), targetType);
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return new VariableReferenceExpression(reference.getName(), targetType);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return specialForm(specialForm.getForm(), targetType, specialForm.getArguments());
        }
    }
}
