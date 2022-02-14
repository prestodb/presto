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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.hive.functions.type.BlockInputDecoder;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.hive.functions.gen.CompilerUtils.defineClass;
import static com.facebook.presto.hive.functions.gen.CompilerUtils.makeClassName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public final class HiveAccumulatorMethodHandles
{
    private static final String INPUT_FUNCTION_CLASS = "HiveAggregationInputFunction";
    private static final String INPUT_FUNCTION_METHOD = "input";

    private static final MethodHandle COMBINE = methodHandle(
            HiveAccumulatorMethodHandles.class,
            "combine",
            HiveAccumulatorInvoker.class,
            HiveAccumulatorState.class,
            HiveAccumulatorState.class);

    private static final MethodHandle OUTPUT = methodHandle(
            HiveAccumulatorMethodHandles.class,
            "output",
            HiveAccumulatorInvoker.class,
            HiveAccumulatorState.class,
            BlockBuilder.class);

    private HiveAccumulatorMethodHandles() {}

    public static MethodHandle getInputFunction(com.facebook.presto.hive.functions.aggregation.HiveAccumulatorInvoker context, List<BlockInputDecoder> decoders)
    {
        requireNonNull(context);
        requireNonNull(decoders);

        MethodHandle methodHandle = generateUnboundInputFunction(decoders.size());
        methodHandle = methodHandle.bindTo(context);
        for (BlockInputDecoder decoder : decoders) {
            methodHandle = methodHandle.bindTo(decoder);
        }
        return methodHandle;
    }

    public static MethodHandle getCombineFunction(HiveAccumulatorInvoker context)
    {
        return COMBINE.bindTo(requireNonNull(context));
    }

    public static MethodHandle getOutputFunction(HiveAccumulatorInvoker context)
    {
        return OUTPUT.bindTo(requireNonNull(context));
    }

    public static MethodHandle generateUnboundInputFunction(int numInputs)
    {
        // Create class
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(INPUT_FUNCTION_CLASS),
                ParameterizedType.type(Object.class));
        classDefinition.declareDefaultConstructor(a(PRIVATE));

        // Declare parameters
        Parameter invokerParameter = arg("invoker", HiveAccumulatorInvoker.class);
        List<Parameter> decoderParameters = range(0, numInputs)
                .mapToObj(i -> arg("decoder_" + i, BlockInputDecoder.class))
                .collect(Collectors.toList());
        Parameter stateParameter = arg("state", HiveAccumulatorState.class);
        List<Parameter> blockParameters = range(0, numInputs)
                .mapToObj(i -> arg("input_" + i, Block.class))
                .collect(Collectors.toList());
        Parameter positionParameter = arg("position", int.class);
        ArrayList<Parameter> parameters = new ArrayList<>(2 * numInputs + 3);
        parameters.add(invokerParameter);
        parameters.addAll(decoderParameters);
        parameters.add(stateParameter);
        parameters.addAll(blockParameters);
        parameters.add(positionParameter);

        // Declare method
        MethodDefinition methodDefinition = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                INPUT_FUNCTION_METHOD,
                ParameterizedType.type(void.class),
                parameters.toArray(new Parameter[0]));

        // Implement method
        BytecodeExpression[] decoded = range(0, numInputs)
                .mapToObj(i -> decoderParameters.get(i).invoke("decode", Object.class, blockParameters.get(i), positionParameter))
                .toArray(BytecodeExpression[]::new);
        methodDefinition.getBody().append(invokerParameter.invoke(
                        "iterate",
                        void.class,
                        stateParameter,
                        newArray(ParameterizedType.type(Object[].class), decoded))
                .ret());

        // Generate class
        Class<?> generatedClass = defineClass(
                classDefinition,
                Object.class,
                Collections.emptyMap(),
                HiveAccumulatorMethodHandles.class.getClassLoader());

        // lookup method
        List<Class<?>> parameterClasses = new ArrayList<>(parameters.size());
        parameterClasses.add(HiveAccumulatorInvoker.class);
        parameterClasses.addAll(range(0, numInputs)
                .mapToObj(i -> BlockInputDecoder.class)
                .collect(Collectors.toList()));
        parameterClasses.add(HiveAccumulatorState.class);
        parameterClasses.addAll(range(0, numInputs)
                .mapToObj(i -> Block.class)
                .collect(Collectors.toList()));
        parameterClasses.add(int.class);
        return methodHandle(generatedClass, INPUT_FUNCTION_METHOD, parameterClasses.toArray(new Class<?>[0]));
    }

    public static void combine(HiveAccumulatorInvoker invoker, HiveAccumulatorState state, HiveAccumulatorState otherState)
    {
        invoker.combine(state, otherState);
    }

    public static void output(HiveAccumulatorInvoker invoker, HiveAccumulatorState state, BlockBuilder out)
    {
        invoker.output(state, out);
    }
}
