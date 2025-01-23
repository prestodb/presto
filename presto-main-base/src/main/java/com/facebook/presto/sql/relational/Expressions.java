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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InSubqueryExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.QuantifiedComparisonExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST_NAME;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.asList;

public final class Expressions
{
    private static final List<String> COMPARISON_FUNCTIONS = Arrays.stream(OperatorType.values())
            .filter(OperatorType::isComparisonOperator)
            .map(operator -> operator.getFunctionName().toString())
            .collect(toImmutableList());

    private Expressions()
    {
    }

    private static Optional<SourceLocation> getFirstSourceLocation(List<RowExpression> expressions)
    {
        return expressions.stream()
                .filter(x -> x.getSourceLocation().isPresent())
                .map(x -> x.getSourceLocation())
                .findFirst()
                .orElse(Optional.empty());
    }

    public static ConstantExpression constant(Object value, Type type)
    {
        return constant(Optional.empty(), value, type);
    }

    public static ConstantExpression constant(Optional<SourceLocation> sourceLocation, Object value, Type type)
    {
        return new ConstantExpression(sourceLocation, value, type);
    }

    public static ConstantExpression constantNull(Type type)
    {
        return constantNull(Optional.empty(), type);
    }

    public static ConstantExpression constantNull(Optional<SourceLocation> sourceLocation, Type type)
    {
        return new ConstantExpression(sourceLocation, null, type);
    }

    public static boolean isNull(RowExpression expression)
    {
        return expression instanceof ConstantExpression && ((ConstantExpression) expression).isNull();
    }

    public static boolean isComparison(CallExpression callExpression)
    {
        return COMPARISON_FUNCTIONS.contains(callExpression.getFunctionHandle().getName());
    }

    public static SpecialFormExpression coalesceNullToFalse(RowExpression rowExpression)
    {
        return coalesce(rowExpression, constant(false, BOOLEAN));
    }

    public static SpecialFormExpression coalesce(RowExpression rowExpression, RowExpression coalesced)
    {
        checkState(rowExpression.getType().equals(coalesced.getType()));
        return new SpecialFormExpression(rowExpression.getSourceLocation(), COALESCE, coalesced.getType(), rowExpression, coalesced);
    }

    public static CallExpression not(FunctionAndTypeManager functionAndTypeManager, RowExpression rowExpression)
    {
        return call(functionAndTypeManager, "not", BOOLEAN, rowExpression);
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, RowExpression... arguments)
    {
        return call(displayName, functionHandle, returnType, asList(arguments));
    }

    public static CallExpression call(Optional<SourceLocation> sourceLocation, String displayName, FunctionHandle functionHandle, Type returnType, RowExpression... arguments)
    {
        return new CallExpression(displayName, functionHandle, returnType, asList(arguments));
    }

    public static CallExpression call(String displayName, FunctionHandle functionHandle, Type returnType, List<RowExpression> arguments)
    {
        return new CallExpression(
                getFirstSourceLocation(arguments),
                displayName,
                functionHandle,
                returnType,
                arguments);
    }

    public static CallExpression call(Optional<SourceLocation> sourceLocation, String displayName, FunctionHandle functionHandle, Type returnType, List<RowExpression> arguments)
    {
        return new CallExpression(sourceLocation, displayName, functionHandle, returnType, arguments);
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

    public static CallExpression call(FunctionAndTypeResolver functionAndTypeResolver, String name, Type returnType, RowExpression... arguments)
    {
        FunctionHandle functionHandle = functionAndTypeResolver.lookupFunction(name, fromTypes(Arrays.stream(arguments).map(RowExpression::getType).collect(toImmutableList())));
        return call(name, functionHandle, returnType, arguments);
    }

    public static CallExpression callOperator(FunctionAndTypeResolver functionAndTypeResolver, OperatorType operatorType, Type returnType, RowExpression... arguments)
    {
        FunctionHandle functionHandle = functionAndTypeResolver.resolveOperator(operatorType, fromTypes(Arrays.stream(arguments).map(RowExpression::getType).collect(toImmutableList())));
        return call(operatorType.name(), functionHandle, returnType, arguments);
    }

    public static RowExpression castToBigInt(FunctionAndTypeManager functionAndTypeManager, RowExpression rowExpression)
    {
        if (rowExpression.getType().equals(BIGINT)) {
            return rowExpression;
        }
        return call("CAST", functionAndTypeManager.lookupCast(CastType.CAST, rowExpression.getType(), BIGINT), BIGINT, rowExpression);
    }

    public static RowExpression castToInteger(FunctionAndTypeManager functionAndTypeManager, RowExpression rowExpression)
    {
        if (rowExpression.getType().equals(INTEGER)) {
            return rowExpression;
        }
        return call("CAST", functionAndTypeManager.lookupCast(CastType.CAST, rowExpression.getType(), INTEGER), INTEGER, rowExpression);
    }

    public static RowExpression tryCast(FunctionAndTypeManager functionAndTypeManager, RowExpression rowExpression, Type castToType)
    {
        return call(TRY_CAST_NAME, functionAndTypeManager.lookupCast(CastType.TRY_CAST, rowExpression.getType(), castToType), castToType, rowExpression);
    }

    public static RowExpression searchedCaseExpression(List<RowExpression> whenClauses, Optional<RowExpression> defaultValue)
    {
        // We rewrite this as - CASE true WHEN p1 THEN v1 WHEN p2 THEN v2 .. ELSE v END
        return buildSwitch(new ConstantExpression(true, BOOLEAN), whenClauses, defaultValue, BOOLEAN);
    }

    public static RowExpression buildSwitch(RowExpression operand, List<RowExpression> whenClauses, Optional<RowExpression> defaultValue, Type returnType)
    {
        ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();

        arguments.add(operand);
        arguments.addAll(whenClauses);

        arguments.add(defaultValue
                .orElseGet(() -> constantNull(operand.getSourceLocation(), returnType)));

        return specialForm(SWITCH, returnType, arguments.build());
    }

    public static RowExpression comparisonExpression(
            StandardFunctionResolution functionResolution,
            OperatorType operatorType,
            RowExpression left,
            RowExpression right)
    {
        return call(
                operatorType.name(),
                functionResolution.comparisonFunction(operatorType, left.getType(), right.getType()),
                BOOLEAN,
                left,
                right);
    }

    public static InputReferenceExpression field(Optional<SourceLocation> sourceLocation, int field, Type type)
    {
        return new InputReferenceExpression(sourceLocation, field, type);
    }

    public static InputReferenceExpression field(int field, Type type)
    {
        return new InputReferenceExpression(Optional.empty(), field, type);
    }

    public static SpecialFormExpression specialForm(Form form, Type returnType, RowExpression... arguments)
    {
        return specialForm(form, returnType, ImmutableList.copyOf(arguments));
    }

    public static SpecialFormExpression specialForm(Optional<SourceLocation> sourceLocation, Form form, Type returnType, RowExpression... arguments)
    {
        return specialForm(sourceLocation, form, returnType, ImmutableList.copyOf(arguments));
    }

    public static SpecialFormExpression specialForm(Form form, Type returnType, List<RowExpression> arguments)
    {
        return specialForm(getFirstSourceLocation(arguments), form, returnType, arguments);
    }

    public static SpecialFormExpression specialForm(Optional<SourceLocation> sourceLocation, Form form, Type returnType, List<RowExpression> arguments)
    {
        return new SpecialFormExpression(sourceLocation, form, returnType, arguments);
    }

    public static InSubqueryExpression inSubquery(VariableReferenceExpression value, VariableReferenceExpression subquery)
    {
        return new InSubqueryExpression(getFirstSourceLocation(asList(value, subquery)), value, subquery);
    }

    public static QuantifiedComparisonExpression quantifiedComparison(
            OperatorType operator,
            QuantifiedComparisonExpression.Quantifier quantifier,
            RowExpression value,
            RowExpression subquery)
    {
        return new QuantifiedComparisonExpression(
                getFirstSourceLocation(asList(value, subquery)),
                operator,
                quantifier,
                value,
                subquery);
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
        return variable(Optional.empty(), name, type);
    }

    public static VariableReferenceExpression variable(Optional<SourceLocation> sourceLocation, String name, Type type)
    {
        return new VariableReferenceExpression(sourceLocation, name, type);
    }
}
