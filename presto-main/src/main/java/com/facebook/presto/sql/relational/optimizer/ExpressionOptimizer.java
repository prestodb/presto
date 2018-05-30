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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static com.facebook.presto.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Signatures.BIND;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static com.facebook.presto.sql.relational.Signatures.COALESCE;
import static com.facebook.presto.sql.relational.Signatures.DEREFERENCE;
import static com.facebook.presto.sql.relational.Signatures.IF;
import static com.facebook.presto.sql.relational.Signatures.IN;
import static com.facebook.presto.sql.relational.Signatures.IS_NULL;
import static com.facebook.presto.sql.relational.Signatures.NULL_IF;
import static com.facebook.presto.sql.relational.Signatures.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.relational.Signatures.SWITCH;
import static com.facebook.presto.sql.relational.Signatures.TRY_CAST;
import static com.facebook.presto.type.JsonType.JSON;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ExpressionOptimizer
{
    private final FunctionRegistry registry;
    private final TypeManager typeManager;
    private final ConnectorSession session;

    public ExpressionOptimizer(FunctionRegistry registry, TypeManager typeManager, Session session)
    {
        this.registry = registry;
        this.typeManager = typeManager;
        this.session = session.toConnectorSession();
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
            if (call.getSignature().getName().equals(CAST)) {
                call = rewriteCast(call);
            }
            Signature signature = call.getSignature();

            switch (signature.getName()) {
                // TODO: optimize these special forms
                case IF: {
                    checkState(call.getArguments().size() == 3, "IF function should have 3 arguments. Get " + call.getArguments().size());
                    RowExpression optimizedOperand = call.getArguments().get(0).accept(this, context);
                    if (optimizedOperand instanceof ConstantExpression) {
                        ConstantExpression constantOperand = (ConstantExpression) optimizedOperand;
                        checkState(constantOperand.getType().equals(BOOLEAN), "Operand of IF function should be BOOLEAN type. Get type " + constantOperand.getType().getDisplayName());
                        if (Boolean.TRUE.equals(constantOperand.getValue())) {
                            return call.getArguments().get(1).accept(this, context);
                        }
                        // FALSE and NULL
                        else {
                            return call.getArguments().get(2).accept(this, context);
                        }
                    }
                    List<RowExpression> arguments = call.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return call(signature, call.getType(), arguments);
                }
                case BIND: {
                    checkState(call.getArguments().size() >= 1, BIND + " function should have at least 1 argument. Got " + call.getArguments().size());

                    boolean allConstantExpression = true;
                    ImmutableList.Builder<RowExpression> optimizedArgumentsBuilder = ImmutableList.builder();
                    for (RowExpression argument : call.getArguments()) {
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
                    return call(signature, call.getType(), optimizedArgumentsBuilder.build());
                }
                case NULL_IF:
                case SWITCH:
                case "WHEN":
                case TRY_CAST:
                case IS_NULL:
                case COALESCE:
                case "AND":
                case "OR":
                case IN:
                case DEREFERENCE:
                case ROW_CONSTRUCTOR: {
                    List<RowExpression> arguments = call.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return call(signature, call.getType(), arguments);
                }
            }

            ScalarFunctionImplementation function = registry.getScalarFunctionImplementation(signature);
            List<RowExpression> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());

            // TODO: optimize function calls with lambda arguments. For example, apply(x -> x + 2, 1)
            if (Iterables.all(arguments, instanceOf(ConstantExpression.class)) && function.isDeterministic()) {
                MethodHandle method = function.getMethodHandle();

                if (method.type().parameterCount() > 0 && method.type().parameterType(0) == ConnectorSession.class) {
                    method = method.bindTo(session);
                }

                int index = 0;
                List<Object> constantArguments = new ArrayList<>();
                for (RowExpression argument : arguments) {
                    Object value = ((ConstantExpression) argument).getValue();
                    // if any argument is null, return null
                    if (value == null && function.getArgumentProperty(index).getNullConvention() == RETURN_NULL_ON_NULL) {
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

            return call(signature, typeManager.getType(signature.getReturnType()), arguments);
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

        private CallExpression rewriteCast(CallExpression call)
        {
            if (call.getArguments().get(0) instanceof CallExpression) {
                // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW)
                CallExpression innerCall = (CallExpression) call.getArguments().get(0);
                if (innerCall.getSignature().getName().equals("json_parse")) {
                    checkArgument(innerCall.getType().equals(JSON));
                    checkArgument(innerCall.getArguments().size() == 1);
                    TypeSignature returnType = call.getSignature().getReturnType();
                    if (returnType.getBase().equals(ARRAY)) {
                        return call(
                                internalScalarFunction(
                                        JSON_STRING_TO_ARRAY_NAME,
                                        returnType,
                                        ImmutableList.of(parseTypeSignature(VARCHAR))),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(MAP)) {
                        return call(
                                internalScalarFunction(
                                        JSON_STRING_TO_MAP_NAME,
                                        returnType,
                                        ImmutableList.of(parseTypeSignature(VARCHAR))),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(ROW)) {
                        return call(
                                internalScalarFunction(
                                        JSON_STRING_TO_ROW_NAME,
                                        returnType,
                                        ImmutableList.of(parseTypeSignature(VARCHAR))),
                                call.getType(),
                                innerCall.getArguments());
                    }
                }
            }

            return call(
                    registry.getCoercion(call.getArguments().get(0).getType(), call.getType()),
                    call.getType(),
                    call.getArguments());
        }
    }
}
