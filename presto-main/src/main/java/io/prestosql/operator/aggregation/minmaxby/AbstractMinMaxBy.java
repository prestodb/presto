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
package io.prestosql.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlAggregationFunction;
import io.prestosql.operator.aggregation.AccumulatorCompiler;
import io.prestosql.operator.aggregation.AggregationMetadata;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.operator.aggregation.GenericAccumulatorFactoryBinder;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.aggregation.state.StateCompiler;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.sql.gen.SqlTypeBytecodeExpression;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.airlift.bytecode.expression.BytecodeExpressions.or;
import static io.prestosql.metadata.Signature.orderableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static io.prestosql.operator.aggregation.AggregationUtils.generateAggregationName;
import static io.prestosql.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateClass;
import static io.prestosql.operator.aggregation.minmaxby.TwoNullableValueStateMapping.getStateSerializer;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.BytecodeUtils.loadConstant;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.methodHandle;

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
        DynamicClassLoader classLoader = new DynamicClassLoader(getClass().getClassLoader());

        // Generate states and serializers:
        // For value that is a Block or Slice, we store them as Block/position combination
        // to avoid generating long-living objects through getSlice or getObject.
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
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateClazz,
                        stateSerializer,
                        stateFactory)),
                valueType);
        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, ImmutableList.of(intermediateType), valueType, true, false, factory);
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
