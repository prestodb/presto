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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassWriter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AccumulatorCompiler
{
    private static final boolean DUMP_BYTE_CODE_TREE = false;

    private static final AtomicLong CLASS_ID = new AtomicLong();

    public static final Set<Class<?>> SUPPORTED_PARAMETER_TYPES = ImmutableSet.of(com.facebook.presto.spi.block.Block.class, long.class, double.class, boolean.class, Slice.class);

    public AccumulatorFactory generateAccumulatorFactory(
            Class<?> aggregationDefinitionClass,
            Class<?> stateClass,
            Type intermediateType,
            Type finalType,
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            boolean approximateSupported)
    {
        DynamicClassLoader classLoader = createClassLoader();
        Class<? extends Accumulator> accumulatorClass = generateAccumulatorClass(aggregationDefinitionClass, stateClass, classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = generateGroupedAccumulatorClass(aggregationDefinitionClass, stateClass, classLoader);
        return new GenericAccumulatorFactory(
                finalType,
                intermediateType,
                stateSerializer,
                stateFactory,
                accumulatorClass,
                groupedAccumulatorClass,
                approximateSupported);
    }

    private static Class<? extends GroupedAccumulator> generateGroupedAccumulatorClass(Class<?> aggregationClass, Class<?> stateClass, DynamicClassLoader classLoader)
    {
        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName(aggregationClass.getSimpleName() + "GroupedAccumulator_" + CLASS_ID.incrementAndGet()),
                type(AbstractGroupedAccumulator.class));

        // Generate constructor
        generateMatchingConstructors(definition, AbstractGroupedAccumulator.class);

        // Generate methods
        generateProcessInput(definition, AbstractGroupedAccumulator.class, aggregationClass, stateClass);
        generateProcessIntermediate(definition, AbstractGroupedAccumulator.class, aggregationClass, stateClass);
        generateEvaluateFinal(definition, AbstractGroupedAccumulator.class, aggregationClass, stateClass);

        return defineClass(definition, AbstractGroupedAccumulator.class, classLoader);
    }

    private static Class<? extends Accumulator> generateAccumulatorClass(Class<?> aggregationClass, Class<?> stateClass, DynamicClassLoader classLoader)
    {
        ClassDefinition definition = new ClassDefinition(new CompilerContext(null),
                a(PUBLIC, FINAL),
                typeFromPathName(aggregationClass.getSimpleName() + "Accumulator_" + CLASS_ID.incrementAndGet()),
                type(AbstractAccumulator.class));

        // Generate constructor
        generateMatchingConstructors(definition, AbstractAccumulator.class);

        // Generate methods
        generateProcessInput(definition, AbstractAccumulator.class, aggregationClass, stateClass);
        generateProcessIntermediate(definition, AbstractAccumulator.class, aggregationClass, stateClass);
        generateEvaluateFinal(definition, AbstractAccumulator.class, aggregationClass, stateClass);

        return defineClass(definition, AbstractAccumulator.class, classLoader);
    }

    private static void generateProcessInput(ClassDefinition definition, Class<?> clazz, Class<?> aggregationClass, Class<?> stateClass)
    {
        Block body = definition.declareMethod(new CompilerContext(null), a(PUBLIC), "processInput", type(void.class), arg("state", AccumulatorState.class), arg("blocks", List.class), arg("position", int.class), arg("sampleWeight", long.class))
                .getBody();

        Method inputFunction = findInputFunction(aggregationClass, stateClass);
        body.comment("Call input function with unpacked Block arguments")
                .getVariable("state")
                .checkCast(stateClass);

        Class<?>[] parameters = inputFunction.getParameterTypes();
        Annotation[][] annotations = inputFunction.getParameterAnnotations();
        for (int i = 1, blockNum = 0; i < parameters.length; i++) {
            if (annotations[i][0] instanceof BlockIndex) {
                body.getVariable("position");
            }
            else {
                body.getVariable("blocks")
                        .push(blockNum)
                        .invokeInterface(List.class, "get", Object.class, int.class)
                        .checkCast(com.facebook.presto.spi.block.Block.class);
                getStackTypeIfNecessary(body, parameters[i]);
                blockNum++;
            }
        }

        body.invokeStatic(inputFunction) .ret();
    }

    // Assumes that there is a variable named 'position' in the block, which is the current index
    private static Block getStackTypeIfNecessary(Block block, Class<?> parameter)
    {
        if (parameter == com.facebook.presto.spi.block.Block.class) {
            return block;
        }

        if (parameter == long.class) {
            block.comment("block.getLong(position)")
                    .getVariable("position")
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getLong", long.class, int.class);
        }
        else if (parameter == double.class) {
            block.comment("block.getDouble(position)")
                    .getVariable("position")
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getDouble", double.class, int.class);
        }
        else if (parameter == boolean.class) {
            block.comment("block.getBoolean(position)")
                    .getVariable("position")
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getBoolean", boolean.class, int.class);
        }
        else if (parameter == Slice.class) {
            block.comment("block.getSlice(position)")
                    .getVariable("position")
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "getSlice", Slice.class, int.class);
        }
        else {
            throw new IllegalArgumentException("Unsupported parameter type: " + parameter.getSimpleName());
        }

        return block;
    }

    private static Method findInputFunction(Class<?> clazz, Class<?> stateClass)
    {
        Method method = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, InputFunction.class);
        checkNotNull(method, "Aggregation function class %s does not have an @InputFunction", clazz.getName());
        verifyInputFunctionSignature(method, stateClass);

        return method;
    }

    private static void verifyInputFunctionSignature(Method method, Class<?> stateClass)
    {
        checkNotNull(method, "method is null");
        Class<?>[] parameters = method.getParameterTypes();
        Annotation[][] annotations = method.getParameterAnnotations();
        checkArgument(stateClass == parameters[0], "First argument of aggregation input function must be %s", stateClass.getSimpleName());
        checkArgument(parameters.length > 1, "Aggregation input function must have at least one parameter");
        for (int i = 1; i < parameters.length; i++) {
            checkArgument(annotations[i].length == 1, "Each parameter (besides state) must have exactly one annotation (either @SqlType or @BlockIndex)");

            Annotation annotation = annotations[i][0];
            if (annotation instanceof SqlType) {
                checkArgument(SUPPORTED_PARAMETER_TYPES.contains(parameters[i]), "Unsupported type: %s", parameters[i].getSimpleName());
            }
            else if (annotation instanceof BlockIndex) {
                checkArgument(parameters[i] == int.class, "Block index parameter must be an int");
            }
            else {
                throw new IllegalArgumentException("Unsupported annotation: " + annotation);
            }
        }
    }

    private static void generateProcessIntermediate(ClassDefinition definition, Class<?> clazz, Class<?> aggregationClass, Class<?> stateClass)
    {
        Block body = definition.declareMethod(
                new CompilerContext(null),
                a(PUBLIC),
                "processIntermediate",
                type(void.class),
                arg("state", AccumulatorState.class),
                arg("scratchState", AccumulatorState.class),
                arg("block", com.facebook.presto.spi.block.Block.class),
                arg("position", int.class))
                .getBody();

        Method combineFunction = findCombineFunction(aggregationClass, stateClass);
        Method intermediateInputFunction = findIntermediateInputFunction(aggregationClass, stateClass);
        checkArgument(combineFunction == null || intermediateInputFunction == null, "Aggregation (%s) cannot have both a combine and a intermediate input method", aggregationClass.getSimpleName());
        checkArgument(combineFunction != null || intermediateInputFunction != null, "Aggregation (%s) must have either a combine or a intermediate input method", aggregationClass.getSimpleName());
        if (combineFunction != null) {
            body.pushThis()
                    .comment("stateSerializer.deserialize(block, position, scratchState)")
                    .getField(clazz, "stateSerializer", AccumulatorStateSerializer.class)
                    .getVariable("block")
                    .getVariable("position")
                    .getVariable("scratchState")
                    .invokeInterface(AccumulatorStateSerializer.class, "deserialize", void.class, com.facebook.presto.spi.block.Block.class, int.class, Object.class)
                    .comment("combine(state, scratchState)")
                    .getVariable("state")
                    .checkCast(stateClass)
                    .getVariable("scratchState")
                    .checkCast(stateClass)
                    .invokeStatic(combineFunction)
                    .ret();
        }
        else {
            body.getVariable("state");
            Class<?>[] parameters = intermediateInputFunction.getParameterTypes();
            Annotation[][] annotations = intermediateInputFunction.getParameterAnnotations();
            boolean parameterFound = false;
            for (int i = 1; i < parameters.length; i++) {
                if (annotations[i][0] instanceof BlockIndex) {
                    body.getVariable("position");
                }
                else {
                    checkArgument(!parameterFound, "Intermediate input functions may only have one parameter");
                    body.getVariable("block");
                    getStackTypeIfNecessary(body, parameters[i]);
                    parameterFound = true;
                }
            }
            body.invokeStatic(intermediateInputFunction)
                    .ret();
        }
    }

    private static Method findIntermediateInputFunction(Class<?> clazz, Class<?> stateClass)
    {
        Method method = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, IntermediateInputFunction.class);
        if (method == null) {
            return null;
        }
        verifyInputFunctionSignature(method, stateClass);
        return method;
    }

    private static Method findCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        Method method = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, CombineFunction.class);
        if (method == null) {
            return null;
        }
        Class<?>[] parameterTypes = method.getParameterTypes();
        checkArgument(parameterTypes.length == 2 && parameterTypes[0] == stateClass && parameterTypes[1] == stateClass, "Combine function must have the signature (%s, %s)", stateClass.getSimpleName(), stateClass.getSimpleName());
        return method;
    }

    private static void generateEvaluateFinal(ClassDefinition definition, Class<?> clazz, Class<?> aggregationClass, Class<?> stateClass)
    {
        Block body = definition.declareMethod(
                new CompilerContext(null),
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                arg("state", AccumulatorState.class),
                arg("confidence", double.class),
                arg("out", BlockBuilder.class))
                .getBody();

        Method outputFunction = findOutputFunction(aggregationClass, stateClass);
        if (outputFunction != null) {
            body.comment("output(state, out)")
                    .getVariable("state")
                    .checkCast(stateClass)
                    .getVariable("out")
                    .invokeStatic(outputFunction)
                    .ret();
        }
        else {
            body.pushThis()
                    .comment("stateSerializer.serialize(state, out)")
                    .getField(clazz, "stateSerializer", AccumulatorStateSerializer.class)
                    .getVariable("state")
                    .getVariable("out")
                    .invokeInterface(AccumulatorStateSerializer.class, "serialize", void.class, Object.class, BlockBuilder.class)
                    .ret();
        }
    }

    private static Method findOutputFunction(Class<?> clazz, Class<?> stateClass)
    {
        Method method = CompilerUtils.findPublicStaticMethodWithAnnotation(clazz, OutputFunction.class);
        if (method == null) {
            return null;
        }
        Class<?>[] parameterTypes = method.getParameterTypes();
        checkArgument(parameterTypes.length == 2 && parameterTypes[0] == stateClass && parameterTypes[1] == BlockBuilder.class, "Output function must have the signature (%s, BlockBuilder)", stateClass.getSimpleName());
        return method;
    }

    private static void generateMatchingConstructors(ClassDefinition definition, Class<?> superClass)
    {
        for (Constructor<?> constructor : superClass.getDeclaredConstructors()) {
            List<NamedParameterDefinition> parameters = new ArrayList<>();
            for (int i = 0; i < constructor.getParameterTypes().length; i++) {
                parameters.add(arg("parameter_" + i, constructor.getParameterTypes()[i]));
            }
            Block body = definition.declareConstructor(new CompilerContext(null), a(PUBLIC), parameters)
                    .getBody()
                    .comment("super(...)")
                    .pushThis();
            for (NamedParameterDefinition parameter : parameters) {
                body.getVariable(parameter.getName());
            }
            body.invokeConstructor(constructor);
            body.ret();
        }
    }

    private DynamicClassLoader createClassLoader()
    {
        return new DynamicClassLoader(getClass().getClassLoader());
    }

    private static Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
    {
        ClassInfoLoader classInfoLoader = ClassInfoLoader.createClassInfoLoader(classDefinitions, classLoader);

        if (DUMP_BYTE_CODE_TREE) {
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(System.out);
            for (ClassDefinition classDefinition : classDefinitions) {
                dumpByteCode.visitClass(classDefinition);
            }
        }

        Map<String, byte[]> byteCodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            classDefinition.visit(cw);
            byte[] byteCode = cw.toByteArray();
            byteCodes.put(classDefinition.getType().getJavaClassName(), byteCode);
        }

        return classLoader.defineClasses(byteCodes);
    }

    private static <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }
}
