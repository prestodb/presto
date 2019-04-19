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
package com.facebook.presto.sql.relational.optimizer;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ARRAY_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_MAP_CAST;
import static com.facebook.presto.metadata.CastType.JSON_TO_ROW_CAST;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST_NAME;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.type.JsonType.JSON;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ExpressionOptimizer
{
    private final FunctionManager functionManager;
    private final ConnectorSession session;

    public ExpressionOptimizer(FunctionManager functionManager, ConnectorSession session)
    {
        this.functionManager = functionManager;
        this.session = session;
    }

    public RowExpression optimize(RowExpression expression)
    {
        return expression.accept(new Visitor(), null);
    }

    private class Visitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            FunctionHandle functionHandle = call.getFunctionHandle();
            FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(functionHandle);
            if (functionMetadata.getName().equals(TRY_CAST_NAME)) {
                List<RowExpression> arguments = call.getArguments().stream()
                        .map(argument -> argument.accept(this, null))
                        .collect(toImmutableList());
                return call(call.getDisplayName(), functionHandle, call.getType(), arguments);
            }
            if (functionMetadata.getName().equals(CAST.getCastName())) {
                call = rewriteCast(call);
                functionHandle = call.getFunctionHandle();
            }

            ScalarFunctionImplementation function = functionManager.getScalarFunctionImplementation(functionHandle);
            List<RowExpression> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());

            // TODO: optimize function calls with lambda arguments. For example, apply(x -> x + 2, 1)
            if (Iterables.all(arguments, instanceOf(ConstantExpression.class)) && functionMetadata.isDeterministic()) {
                MethodHandle method = function.getMethodHandle();

                if (method.type().parameterCount() > 0 && method.type().parameterType(0) == ConnectorSession.class) {
                    method = method.bindTo(session);
                }

                int index = 0;
                List<Object> constantArguments = new ArrayList<>();
                for (RowExpression argument : arguments) {
                    Object value = ((ConstantExpression) argument).getValue();
                    // if any argument is null, return null
                    if (value == null && !functionMetadata.isCalledOnNullInput()) {
                        return constantNull(call.getType());
                    }
                    constantArguments.add(value);
                    index++;
                }

                try {
                    return constant(method.invokeWithArguments(constantArguments), call.getType());
                }
                catch (Throwable e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    // Do nothing. As a result, this specific tree will be left untouched. But irrelevant expressions will continue to get evaluated and optimized.
                }
            }

            return call(call.getDisplayName(), functionHandle, call.getType(), arguments);
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            Form form = specialForm.getForm();
            switch (form) {
                // TODO: optimize these special forms
                case IF: {
                    checkState(specialForm.getArguments().size() == 3, "IF function should have 3 arguments. Get " + specialForm.getArguments().size());
                    RowExpression optimizedOperand = specialForm.getArguments().get(0).accept(this, context);
                    if (optimizedOperand instanceof ConstantExpression) {
                        ConstantExpression constantOperand = (ConstantExpression) optimizedOperand;
                        checkState(constantOperand.getType().equals(BOOLEAN), "Operand of IF function should be BOOLEAN type. Get type " + constantOperand.getType().getDisplayName());
                        if (Boolean.TRUE.equals(constantOperand.getValue())) {
                            return specialForm.getArguments().get(1).accept(this, context);
                        }
                        // FALSE and NULL
                        else {
                            return specialForm.getArguments().get(2).accept(this, context);
                        }
                    }
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialFormExpression(form, specialForm.getType(), arguments);
                }
                case BIND: {
                    checkState(specialForm.getArguments().size() >= 1, BIND.name() + " function should have at least 1 argument. Got " + specialForm.getArguments().size());

                    boolean allConstantExpression = true;
                    ImmutableList.Builder<RowExpression> optimizedArgumentsBuilder = ImmutableList.builder();
                    for (RowExpression argument : specialForm.getArguments()) {
                        RowExpression optimizedArgument = argument.accept(this, context);
                        if (!(optimizedArgument instanceof ConstantExpression)) {
                            allConstantExpression = false;
                        }
                        optimizedArgumentsBuilder.add(optimizedArgument);
                    }
                    if (allConstantExpression) {
                        // Here, optimizedArguments should be merged together into a new ConstantExpression.
                        // It's not implemented because it would be dead code anyways because visitLambda does not produce ConstantExpression.
                        throw new UnsupportedOperationException();
                    }
                    return new SpecialFormExpression(form, specialForm.getType(), optimizedArgumentsBuilder.build());
                }
                case NULL_IF:
                case SWITCH:
                case WHEN:
                case IS_NULL:
                case COALESCE:
                case AND:
                case OR:
                case IN:
                case DEREFERENCE:
                case ROW_CONSTRUCTOR: {
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialFormExpression(form, specialForm.getType(), arguments);
                }
                default:
                    throw new IllegalArgumentException("Unsupported special form " + specialForm.getForm());
            }
        }

        private CallExpression rewriteCast(CallExpression call)
        {
            if (call.getArguments().get(0) instanceof CallExpression) {
                // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW)
                CallExpression innerCall = (CallExpression) call.getArguments().get(0);
                if (functionManager.getFunctionMetadata(innerCall.getFunctionHandle()).getName().equals("json_parse")) {
                    checkArgument(innerCall.getType().equals(JSON));
                    checkArgument(innerCall.getArguments().size() == 1);
                    TypeSignature returnType = functionManager.getFunctionMetadata(call.getFunctionHandle()).getReturnType();
                    if (returnType.getBase().equals(ARRAY)) {
                        return call(
                                JSON_TO_ARRAY_CAST.name(),
                                functionManager.lookupCast(
                                        JSON_TO_ARRAY_CAST,
                                        parseTypeSignature(VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(MAP)) {
                        return call(
                                JSON_TO_MAP_CAST.name(),
                                functionManager.lookupCast(
                                        JSON_TO_MAP_CAST,
                                        parseTypeSignature(VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(ROW)) {
                        return call(
                                JSON_TO_ROW_CAST.name(),
                                functionManager.lookupCast(
                                        JSON_TO_ROW_CAST,
                                        parseTypeSignature(VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                }
            }

            return call(
                    CAST.name(),
                    functionManager.lookupCast(
                            CAST,
                            call.getArguments().get(0).getType().getTypeSignature(),
                            call.getType().getTypeSignature()),
                    call.getType(),
                    call.getArguments());
        }
    }
}
