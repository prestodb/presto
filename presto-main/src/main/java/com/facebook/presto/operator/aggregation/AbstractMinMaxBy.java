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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.SqlTypeBytecodeExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.or;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.state.TwoNullableValueStateMapping.getStateClass;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public abstract class AbstractMinMaxBy
        extends SqlAggregationFunction
{
    private final boolean min;

    protected AbstractMinMaxBy(boolean min)
    {
        super((min ? "min" : "max") + "_by",
                ImmutableList.of(orderableTypeParameter("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("V"), parseTypeSignature("K")));
        this.min = min;
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        return generateAggregation(valueType, keyType, functionRegistry);
    }

    private InternalAggregationFunction generateAggregation(Type valueType, Type keyType, FunctionRegistry functionRegistry)
    {
        Class<?> stateClazz = getStateClass(keyType.getJavaType(), valueType.getJavaType());
        Map<String, Type> stateFieldTypes = ImmutableMap.of("First", keyType, "Second", valueType);
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());
        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(stateClazz, stateFieldTypes, classLoader);
        AccumulatorStateSerializer<?> stateSerializer = StateCompiler.generateStateSerializer(stateClazz, stateFieldTypes, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        CallSiteBinder binder = new CallSiteBinder();
        OperatorType operator = min ? LESS_THAN : GREATER_THAN;
        MethodHandle compareMethod = functionRegistry.getScalarFunctionImplementation(functionRegistry.resolveOperator(operator, ImmutableList.of(keyType, keyType))).getMethodHandle();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("processMaxOrMinBy"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));
        generateInputMethod(definition, binder, compareMethod, keyType, valueType, stateClazz);
        generateCombineMethod(definition, binder, compareMethod, keyType, valueType, stateClazz);
        generateOutputMethod(definition, binder, valueType, stateClazz);
        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), classLoader);
        MethodHandle inputMethod = methodHandle(generatedClass, "input", stateClazz, Block.class, Block.class, int.class);
        MethodHandle combineMethod = methodHandle(generatedClass, "combine", stateClazz, stateClazz);
        MethodHandle outputMethod = methodHandle(generatedClass, "output", stateClazz, BlockBuilder.class);
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), valueType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(valueType, keyType),
                inputMethod,
                combineMethod,
                outputMethod,
                stateClazz,
                stateSerializer,
                stateFactory,
                valueType);
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, intermediateType, valueType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value, Type key)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INPUT_CHANNEL, key), new ParameterMetadata(BLOCK_INDEX));
    }

    private void generateInputMethod(ClassDefinition definition, CallSiteBinder binder, MethodHandle compareMethod, Type keyType, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter value = arg("value", Block.class);
        Parameter key = arg("key", Block.class);
        Parameter position = arg("position", int.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "input", type(void.class), state, value, key, position);
        if (keyType.equals(UNKNOWN)) {
            method.getBody().ret();
            return;
        }
        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(state.invoke("setFirst", void.class, keySqlType.getValue(key, position)))
                .append(state.invoke("setFirstNull", void.class, constantBoolean(false)))
                .append(state.invoke("setSecondNull", void.class, value.invoke("isNull", boolean.class, position)));
        if (!valueType.equals(UNKNOWN)) {
            SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
            ifBlock.append(new IfStatement()
                    .condition(value.invoke("isNull", boolean.class, position))
                    .ifFalse(state.invoke("setSecond", void.class, valueSqlType.getValue(value, position))));
        }

        method.getBody().append(new IfStatement()
                .condition(or(
                        state.invoke("isFirstNull", boolean.class),
                        and(
                                not(key.invoke("isNull", boolean.class, position)),
                                loadConstant(binder, compareMethod, MethodHandle.class).invoke("invokeExact", boolean.class, keySqlType.getValue(key, position), state.invoke("getFirst", keyType.getJavaType())))))
                .ifTrue(ifBlock))
                .ret();
    }

    private void generateCombineMethod(ClassDefinition definition, CallSiteBinder binder, MethodHandle compareMethod, Type keyType, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter otherState = arg("otherState", stateClass);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "combine", type(void.class), state, otherState);
        if (keyType.equals(UNKNOWN)) {
            method.getBody().ret();
            return;
        }

        Class<?> keyJavaType = keyType.getJavaType();

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(state.invoke("setFirst", void.class, otherState.invoke("getFirst", keyJavaType)))
                .append(state.invoke("setFirstNull", void.class, otherState.invoke("isFirstNull", boolean.class)))
                .append(state.invoke("setSecondNull", void.class, otherState.invoke("isSecondNull", boolean.class)));
        if (!valueType.equals(UNKNOWN)) {
            ifBlock.append(state.invoke("setSecond", void.class, otherState.invoke("getSecond", valueType.getJavaType())));
        }

        method.getBody()
                .append(new IfStatement()
                        .condition(or(
                                state.invoke("isFirstNull", boolean.class),
                                and(
                                        not(otherState.invoke("isFirstNull", boolean.class)),
                                        loadConstant(binder, compareMethod, MethodHandle.class).invoke("invokeExact", boolean.class, otherState.invoke("getFirst", keyJavaType), state.invoke("getFirst", keyJavaType)))))
                        .ifTrue(ifBlock))
                .ret();
    }

    private void generateOutputMethod(ClassDefinition definition, CallSiteBinder binder, Type valueType, Class<?> stateClass)
    {
        Parameter state = arg("state", stateClass);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "output", type(void.class), state, out);

        IfStatement ifStatement = new IfStatement()
                .condition(or(state.invoke("isFirstNull", boolean.class), state.invoke("isSecondNull", boolean.class)))
                .ifTrue(new BytecodeBlock().append(out.invoke("appendNull", BlockBuilder.class)).pop());
        if (!valueType.equals(UNKNOWN)) {
            ifStatement.ifFalse(constantType(binder, valueType).writeValue(out, state.invoke("getSecond", valueType.getJavaType())));
        }
        method.getBody().append(ifStatement).ret();
    }
}
