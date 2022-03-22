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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class Expressions
{
    private Expressions()
    {
    }

    public static ConstantExpression constant(Object value, Type type)
    {
        return new ConstantExpression(value, type);
    }

    public static ConstantExpression constantNull(Type type)
    {
        return new ConstantExpression(null, type);
    }

    public static boolean isNull(RowExpression expression)
    {
        return expression instanceof ConstantExpression && ((ConstantExpression) expression).isNull();
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, RowExpression... arguments)
    {
        return new CallExpression(displayName, functionHandle, returnType, Arrays.asList(arguments));
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, List<RowExpression> arguments)
    {
        return new CallExpression(displayName, functionHandle, returnType, arguments);
    }

    public static CallExpression call(FunctionAndTypeManager functionAndTypeManager, String name, Type returnType, RowExpression... arguments)
    {
        return call(functionAndTypeManager, name, returnType, ImmutableList.copyOf(arguments));
    }

    public static CallExpression call(FunctionAndTypeManager functionAndTypeManager, String name, Type returnType, List<RowExpression> arguments)
    {
        FunctionHandle functionHandle = functionAndTypeManager.lookupFunction(name, fromTypes(arguments.stream().map(RowExpression::getType).collect(toImmutableList())));
        return call(name, functionHandle, returnType, arguments);
    }

    public static InputReferenceExpression field(int field, Type type)
    {
        return new InputReferenceExpression(field, type);
    }

    public static SpecialFormExpression specialForm(Form form, Type returnType, RowExpression... arguments)
    {
        return new SpecialFormExpression(form, returnType, arguments);
    }

    public static SpecialFormExpression specialForm(Form form, Type returnType, List<RowExpression> arguments)
    {
        return new SpecialFormExpression(form, returnType, arguments);
    }

    public static Set<RowExpression> uniqueSubExpressions(RowExpression expression)
    {
        return ImmutableSet.copyOf(subExpressions(ImmutableList.of(expression)));
    }

    public static List<RowExpression> subExpressions(RowExpression expression)
    {
        return subExpressions(ImmutableList.of(expression));
    }

    public static List<RowExpression> subExpressions(Iterable<RowExpression> expressions)
    {
        final ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();

        for (RowExpression expression : expressions) {
            expression.accept(new RowExpressionVisitor<Void, Void>()
            {
                @Override
                public Void visitCall(CallExpression call, Void context)
                {
                    builder.add(call);
                    for (RowExpression argument : call.getArguments()) {
                        argument.accept(this, context);
                    }
                    return null;
                }

                @Override
                public Void visitInputReference(InputReferenceExpression reference, Void context)
                {
                    builder.add(reference);
                    return null;
                }

                @Override
                public Void visitConstant(ConstantExpression literal, Void context)
                {
                    builder.add(literal);
                    return null;
                }

                @Override
                public Void visitLambda(LambdaDefinitionExpression lambda, Void context)
                {
                    builder.add(lambda);
                    lambda.getBody().accept(this, context);
                    return null;
                }

                @Override
                public Void visitVariableReference(VariableReferenceExpression reference, Void context)
                {
                    builder.add(reference);
                    return null;
                }

                @Override
                public Void visitSpecialForm(SpecialFormExpression specialForm, Void context)
                {
                    builder.add(specialForm);
                    for (RowExpression argument : specialForm.getArguments()) {
                        argument.accept(this, context);
                    }
                    return null;
                }
            }, null);
        }

        return builder.build();
    }

    public static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(name, type);
    }
}
