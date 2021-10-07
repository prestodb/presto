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
package com.facebook.presto.operator.aggregation.minmaxby;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
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
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.or;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateClass;
import static com.facebook.presto.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateSerializer;
import static com.facebook.presto.spi.function.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
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
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        return generateAggregation(valueType, keyType, functionAndTypeManager);
    }

    private InternalAggregationFunction generateAggregation(Type valueType, Type keyType, FunctionAndTypeManager functionAndTypeManager)
    {
        Class<?> stateClazz = getStateClass(keyType.getJavaType(), valueType.getJavaType());
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        // Generate states and serializers:
        // For value that is a Block or Slice, we store them as Block/position combination
        // to avoid generating long-living objects through getSlice or getBlock.
        // This can also help reducing cross-region reference in G1GC engine.
        // TODO: keys can have the same problem. But usually they are primitive types (given the nature of comparison).
        AccumulatorStateFactory<?> stateFactory;
        AccumulatorStateSerializer<?> stateSerializer;
        if (valueType.getJavaType().isPrimitive()) {
            Map<String, Type> stateFieldTypes = ImmutableMap.of("First", keyType, "Second", valueType);
            stateFactory = StateCompiler.generateStateFactory(stateClazz, stateFieldTypes, classLoader);
            stateSerializer = StateCompiler.generateStateSerializer(stateClazz, stateFieldTypes, classLoader);
        }
        else {
            // StateCompiler checks type compatibility.
            // Given "Second" in this case is always a Block, we only need to make sure the getter and setter of the Blocks are properly generated.
            // We deliberately make "SecondBlock" an array type so that the compiler will treat it as a block to workaround the sanity check.
            stateFactory = StateCompiler.generateStateFactory(stateClazz, ImmutableMap.of("First", keyType, "SecondBlock", new ArrayType(valueType)), classLoader);

            // States can be generated by StateCompiler given the they are simply classes with getters and setters.
            // However, serializers have logic in it. Creating serializers is better than generating them.
            stateSerializer = getStateSerializer(keyType, valueType);
        }

        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(valueType, keyType);

        CallSiteBinder binder = new CallSiteBinder();
        OperatorType operator = min ? LESS_THAN : GREATER_THAN;
        MethodHandle compareMethod = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperator(operator, fromTypes(keyType, keyType))).getMethodHandle();

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
                generateAggregationName(getSignature().getNameSuffix(), valueType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(valueType, keyType),
                inputMethod,
                combineMethod,
                outputMethod,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateClazz,
                        stateSerializer,
                        stateFactory)),
                valueType);
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getNameSuffix(), inputTypes, ImmutableList.of(intermediateType), valueType, true, false, factory);
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
        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(state.invoke("setFirst", void.class, keySqlType.getValue(key, position)))
                .append(state.invoke("setFirstNull", void.class, constantBoolean(false)))
                .append(state.invoke("setSecondNull", void.class, value.invoke("isNull", boolean.class, position)));
        BytecodeNode setValueNode;
        if (valueType.getJavaType().isPrimitive()) {
            SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
            setValueNode = state.invoke("setSecond", void.class, valueSqlType.getValue(value, position));
        }
        else {
            // Do not get value directly given it creates object overhead.
            // Such objects would live long enough in Block or SliceBigArray to cause GC pressure.
            setValueNode = new BytecodeBlock()
                    .append(state.invoke("setSecondBlock", void.class, value))
                    .append(state.invoke("setSecondPosition", void.class, position));
        }
        ifBlock.append(new IfStatement()
                .condition(value.invoke("isNull", boolean.class, position))
                .ifFalse(setValueNode));

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

        Class<?> keyJavaType = keyType.getJavaType();

        BytecodeBlock ifBlock = new BytecodeBlock()
                .append(state.invoke("setFirst", void.class, otherState.invoke("getFirst", keyJavaType)))
                .append(state.invoke("setFirstNull", void.class, otherState.invoke("isFirstNull", boolean.class)))
                .append(state.invoke("setSecondNull", void.class, otherState.invoke("isSecondNull", boolean.class)));
        if (valueType.getJavaType().isPrimitive()) {
            ifBlock.append(state.invoke("setSecond", void.class, otherState.invoke("getSecond", valueType.getJavaType())));
        }
        else {
            ifBlock.append(new BytecodeBlock()
                    .append(state.invoke("setSecondBlock", void.class, otherState.invoke("getSecondBlock", Block.class)))
                    .append(state.invoke("setSecondPosition", void.class, otherState.invoke("getSecondPosition", int.class))));
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
        SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
        BytecodeExpression getValueExpression;
        if (valueType.getJavaType().isPrimitive()) {
            getValueExpression = state.invoke("getSecond", valueType.getJavaType());
        }
        else {
            getValueExpression = valueSqlType.getValue(state.invoke("getSecondBlock", Block.class), state.invoke("getSecondPosition", int.class));
        }
        ifStatement.ifFalse(valueSqlType.writeValue(out, getValueExpression));
        method.getBody().append(ifStatement).ret();
    }
}
