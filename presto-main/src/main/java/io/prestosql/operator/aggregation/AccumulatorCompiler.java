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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.expression.BytecodeExpressions;
import io.airlift.slice.Slice;
import io.prestosql.operator.GroupByIdBlock;
import io.prestosql.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.function.WindowIndex;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.Binding;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.sql.gen.CompilerOperations;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.not;
import static io.prestosql.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static io.prestosql.operator.aggregation.AggregationMetadata.countInputChannels;
import static io.prestosql.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccumulatorCompiler
{
    private AccumulatorCompiler()
    {
    }

    public static GenericAccumulatorFactoryBinder generateAccumulatorFactoryBinder(AggregationMetadata metadata, DynamicClassLoader classLoader)
    {
        Class<? extends Accumulator> accumulatorClass = generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);

        Class<? extends GroupedAccumulator> groupedAccumulatorClass = generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);

        return new GenericAccumulatorFactoryBinder(
                metadata.getAccumulatorStateDescriptors(),
                accumulatorClass,
                groupedAccumulatorClass);
    }

    private static <T> Class<? extends T> generateAccumulatorClass(
            Class<T> accumulatorInterface,
            AggregationMetadata metadata,
            DynamicClassLoader classLoader)
    {
        boolean grouped = accumulatorInterface == GroupedAccumulator.class;

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(metadata.getName() + accumulatorInterface.getSimpleName()),
                type(Object.class),
                type(accumulatorInterface));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        List<AccumulatorStateDescriptor> stateDescriptors = metadata.getAccumulatorStateDescriptors();
        List<StateFieldAndDescriptor> stateFieldAndDescriptors = new ArrayList<>();
        for (int i = 0; i < stateDescriptors.size(); i++) {
            stateFieldAndDescriptors.add(new StateFieldAndDescriptor(
                    definition.declareField(a(PRIVATE, FINAL), "stateSerializer_" + i, AccumulatorStateSerializer.class),
                    definition.declareField(a(PRIVATE, FINAL), "stateFactory_" + i, AccumulatorStateFactory.class),
                    definition.declareField(a(PRIVATE, FINAL), "state_" + i, grouped ? stateDescriptors.get(i).getFactory().getGroupedStateClass() : stateDescriptors.get(i).getFactory().getSingleStateClass()),
                    stateDescriptors.get(i)));
        }
        List<FieldDefinition> stateFileds = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        int lambdaCount = metadata.getLambdaInterfaces().size();
        List<FieldDefinition> lambdaProviderFields = new ArrayList<>(lambdaCount);
        for (int i = 0; i < lambdaCount; i++) {
            lambdaProviderFields.add(definition.declareField(a(PRIVATE, FINAL), "lambdaProvider_" + i, LambdaProvider.class));
        }

        FieldDefinition inputChannelsField = definition.declareField(a(PRIVATE, FINAL), "inputChannels", type(List.class, Integer.class));
        FieldDefinition maskChannelField = definition.declareField(a(PRIVATE, FINAL), "maskChannel", type(Optional.class, Integer.class));

        // Generate constructor
        generateConstructor(
                definition,
                stateFieldAndDescriptors,
                inputChannelsField,
                maskChannelField,
                lambdaProviderFields,
                grouped);

        // Generate methods
        generateAddInput(
                definition,
                stateFileds,
                inputChannelsField,
                maskChannelField,
                metadata.getValueInputMetadata(),
                metadata.getLambdaInterfaces(),
                lambdaProviderFields,
                metadata.getInputFunction(),
                callSiteBinder,
                grouped);
        generateAddInputWindowIndex(
                definition,
                stateFileds,
                metadata.getValueInputMetadata(),
                metadata.getLambdaInterfaces(),
                lambdaProviderFields,
                metadata.getInputFunction(),
                callSiteBinder);
        generateGetEstimatedSize(definition, stateFileds);

        generateGetIntermediateType(
                definition,
                callSiteBinder,
                stateDescriptors.stream()
                        .map(stateDescriptor -> stateDescriptor.getSerializer().getSerializedType())
                        .collect(toImmutableList()));

        generateGetFinalType(definition, callSiteBinder, metadata.getOutputType());

        generateAddIntermediateAsCombine(
                definition,
                stateFieldAndDescriptors,
                metadata.getLambdaInterfaces(),
                lambdaProviderFields,
                metadata.getCombineFunction(),
                callSiteBinder,
                grouped);

        if (grouped) {
            generateGroupedEvaluateIntermediate(definition, stateFieldAndDescriptors);
        }
        else {
            generateEvaluateIntermediate(definition, stateFieldAndDescriptors);
        }

        if (grouped) {
            generateGroupedEvaluateFinal(definition, stateFileds, metadata.getOutputFunction(), callSiteBinder);
        }
        else {
            generateEvaluateFinal(definition, stateFileds, metadata.getOutputFunction(), callSiteBinder);
        }

        if (grouped) {
            generatePrepareFinal(definition);
        }
        return defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
    }

    private static MethodDefinition generateGetIntermediateType(ClassDefinition definition, CallSiteBinder callSiteBinder, List<Type> type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getIntermediateType", type(Type.class));

        if (type.size() == 1) {
            methodDefinition.getBody()
                    .append(constantType(callSiteBinder, getOnlyElement(type)))
                    .retObject();
        }
        else {
            methodDefinition.getBody()
                    .append(constantType(callSiteBinder, RowType.anonymous(type)))
                    .retObject();
        }

        return methodDefinition;
    }

    private static MethodDefinition generateGetFinalType(ClassDefinition definition, CallSiteBinder callSiteBinder, Type type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getFinalType", type(Type.class));

        methodDefinition.getBody()
                .append(constantType(callSiteBinder, type))
                .retObject();

        return methodDefinition;
    }

    private static void generateGetEstimatedSize(ClassDefinition definition, List<FieldDefinition> stateFields)
    {
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        Variable estimatedSize = method.getScope().declareVariable(long.class, "estimatedSize");
        method.getBody().append(estimatedSize.set(constantLong(0L)));

        for (FieldDefinition stateField : stateFields) {
            method.getBody()
                    .append(estimatedSize.set(
                            BytecodeExpressions.add(
                                    estimatedSize,
                                    method.getThis().getField(stateField).invoke("getEstimatedSize", long.class))));
        }
        method.getBody().append(estimatedSize.ret());
    }

    private static void generateAddInput(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            FieldDefinition inputChannelsField,
            FieldDefinition maskChannelField,
            List<ParameterMetadata> parameterMetadatas,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        Parameter page = arg("page", Page.class);
        parameters.add(page);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        List<Variable> parameterVariables = new ArrayList<>();
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            parameterVariables.add(scope.declareVariable(Block.class, "block" + i));
        }
        Variable masksBlock = scope.declareVariable(Block.class, "masksBlock");
        body.comment("masksBlock = maskChannel.map(page.blockGetter()).orElse(null);")
                .append(thisVariable.getField(maskChannelField))
                .append(page)
                .invokeStatic(type(AggregationUtils.class), "pageBlockGetter", type(Function.class, Integer.class, Block.class), type(Page.class))
                .invokeVirtual(Optional.class, "map", Optional.class, Function.class)
                .pushNull()
                .invokeVirtual(Optional.class, "orElse", Object.class, Object.class)
                .checkCast(Block.class)
                .putVariable(masksBlock);

        // Get all parameter blocks
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            body.comment("%s = page.getBlock(inputChannels.get(%d));", parameterVariables.get(i).getName(), i)
                    .append(page)
                    .append(thisVariable.getField(inputChannelsField))
                    .push(i)
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(Integer.class)
                    .invokeVirtual(Integer.class, "intValue", int.class)
                    .invokeVirtual(Page.class, "getBlock", Block.class, int.class)
                    .putVariable(parameterVariables.get(i));
        }
        BytecodeBlock block = generateInputForLoop(
                stateField,
                parameterMetadatas,
                inputFunction,
                scope,
                parameterVariables,
                lambdaInterfaces,
                lambdaProviderFields,
                masksBlock,
                callSiteBinder,
                grouped);

        body.append(block);
        body.ret();
    }

    private static void generateAddInputWindowIndex(
            ClassDefinition definition,
            List<FieldDefinition> stateField,
            List<ParameterMetadata> parameterMetadatas,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder)
    {
        // TODO: implement masking based on maskChannel field once Window Functions support DISTINCT arguments to the functions.

        Parameter index = arg("index", WindowIndex.class);
        Parameter channels = arg("channels", type(List.class, Integer.class));
        Parameter startPosition = arg("startPosition", int.class);
        Parameter endPosition = arg("endPosition", int.class);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), ImmutableList.of(index, channels, startPosition, endPosition));
        Scope scope = method.getScope();

        Variable position = scope.declareVariable(int.class, "position");

        Binding binding = callSiteBinder.bind(inputFunction);
        BytecodeExpression invokeInputFunction = invokeDynamic(
                BOOTSTRAP_METHOD,
                ImmutableList.of(binding.getBindingId()),
                "input",
                binding.getType(),
                getInvokeFunctionOnWindowIndexParameters(
                        scope,
                        inputFunction.type().parameterArray(),
                        parameterMetadatas,
                        lambdaInterfaces,
                        lambdaProviderFields,
                        stateField,
                        index,
                        channels,
                        position));

        method.getBody()
                .append(new ForLoop()
                        .initialize(position.set(startPosition))
                        .condition(BytecodeExpressions.lessThanOrEqual(position, endPosition))
                        .update(position.increment())
                        .body(new IfStatement()
                                .condition(anyParametersAreNull(parameterMetadatas, index, channels, position))
                                .ifFalse(invokeInputFunction)))
                .ret();
    }

    private static BytecodeExpression anyParametersAreNull(
            List<ParameterMetadata> parameterMetadatas,
            Variable index,
            Variable channels,
            Variable position)
    {
        int inputChannel = 0;

        BytecodeExpression isNull = constantFalse();
        for (ParameterMetadata parameterMetadata : parameterMetadatas) {
            switch (parameterMetadata.getParameterType()) {
                case BLOCK_INPUT_CHANNEL:
                case INPUT_CHANNEL:
                    BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(inputChannel)).cast(int.class);
                    isNull = BytecodeExpressions.or(isNull, index.invoke("isNull", boolean.class, getChannel, position));
                    inputChannel++;
                    break;
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    inputChannel++;
                    break;
            }
        }

        return isNull;
    }

    private static List<BytecodeExpression> getInvokeFunctionOnWindowIndexParameters(
            Scope scope,
            Class<?>[] parameterTypes,
            List<ParameterMetadata> parameterMetadatas,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            List<FieldDefinition> stateField,
            Variable index,
            Variable channels,
            Variable position)
    {
        int inputChannel = 0;
        int stateIndex = 0;
        verify(parameterTypes.length == parameterMetadatas.size() + lambdaInterfaces.size());
        List<BytecodeExpression> expressions = new ArrayList<>();

        for (int i = 0; i < parameterMetadatas.size(); i++) {
            ParameterMetadata parameterMetadata = parameterMetadatas.get(i);
            Class<?> parameterType = parameterTypes[i];
            BytecodeExpression getChannel = channels.invoke("get", Object.class, constantInt(inputChannel)).cast(int.class);
            switch (parameterMetadata.getParameterType()) {
                case STATE:
                    expressions.add(scope.getThis().getField(stateField.get(stateIndex)));
                    stateIndex++;
                    break;
                case BLOCK_INDEX:
                    // index.getSingleValueBlock(channel, position) generates always a page with only one position
                    expressions.add(constantInt(0));
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    expressions.add(index.invoke(
                            "getSingleValueBlock",
                            Block.class,
                            getChannel,
                            position));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    if (parameterType == long.class) {
                        expressions.add(index.invoke("getLong", long.class, getChannel, position));
                    }
                    else if (parameterType == double.class) {
                        expressions.add(index.invoke("getDouble", double.class, getChannel, position));
                    }
                    else if (parameterType == boolean.class) {
                        expressions.add(index.invoke("getBoolean", boolean.class, getChannel, position));
                    }
                    else if (parameterType == Slice.class) {
                        expressions.add(index.invoke("getSlice", Slice.class, getChannel, position));
                    }
                    else if (parameterType == Block.class) {
                        // Even though the method signature requires a Block parameter, we can pass an Object here.
                        // A runtime check will assert that the Object passed as a parameter is actually of type Block.
                        expressions.add(index.invoke("getObject", Object.class, getChannel, position));
                    }
                    else {
                        throw new IllegalArgumentException(format("Unsupported parameter type: %s", parameterType));
                    }

                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterMetadata.getParameterType());
            }
        }

        for (int i = 0; i < lambdaInterfaces.size(); i++) {
            expressions.add(scope.getThis().getField(lambdaProviderFields.get(i))
                    .invoke("getLambda", Object.class)
                    .cast(lambdaInterfaces.get(i)));
        }

        return expressions;
    }

    private static BytecodeBlock generateInputForLoop(
            List<FieldDefinition> stateField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            Scope scope,
            List<Variable> parameterVariables,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            Variable masksBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable page = scope.getVariable("page");
        Variable positionVariable = scope.declareVariable(int.class, "position");
        Variable rowsVariable = scope.declareVariable(int.class, "rows");

        BytecodeBlock block = new BytecodeBlock()
                .append(page)
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);

        BytecodeNode loopBody = generateInvokeInputFunction(
                scope,
                stateField,
                positionVariable,
                parameterVariables,
                parameterMetadatas,
                lambdaInterfaces,
                lambdaProviderFields,
                inputFunction,
                callSiteBinder,
                grouped);

        //  Wrap with null checks
        List<Boolean> nullable = new ArrayList<>();
        for (ParameterMetadata metadata : parameterMetadatas) {
            switch (metadata.getParameterType()) {
                case INPUT_CHANNEL:
                case BLOCK_INPUT_CHANNEL:
                    nullable.add(false);
                    break;
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    nullable.add(true);
                    break;
                default: // do nothing
            }
        }
        checkState(nullable.size() == parameterVariables.size(), "Number of parameters does not match");
        for (int i = 0; i < parameterVariables.size(); i++) {
            if (!nullable.get(i)) {
                Variable variableDefinition = parameterVariables.get(i);
                loopBody = new IfStatement("if(!%s.isNull(position))", variableDefinition.getName())
                        .condition(new BytecodeBlock()
                                .getVariable(variableDefinition)
                                .getVariable(positionVariable)
                                .invokeInterface(Block.class, "isNull", boolean.class, int.class))
                        .ifFalse(loopBody);
            }
        }

        loopBody = new IfStatement("if(testMask(%s, position))", masksBlock.getName())
                .condition(new BytecodeBlock()
                        .getVariable(masksBlock)
                        .getVariable(positionVariable)
                        .invokeStatic(CompilerOperations.class, "testMask", boolean.class, Block.class, int.class))
                .ifTrue(loopBody);

        block.append(new ForLoop()
                .initialize(new BytecodeBlock().putVariable(positionVariable, 0))
                .condition(new BytecodeBlock()
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(loopBody));

        return block;
    }

    private static BytecodeBlock generateInvokeInputFunction(
            Scope scope,
            List<FieldDefinition> stateField,
            Variable position,
            List<Variable> parameterVariables,
            List<ParameterMetadata> parameterMetadatas,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        BytecodeBlock block = new BytecodeBlock();

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(scope, stateField, block);
        }

        block.comment("Call input function with unpacked Block arguments");

        Class<?>[] parameters = inputFunction.type().parameterArray();
        int inputChannel = 0;
        int stateIndex = 0;
        verify(parameters.length == parameterMetadatas.size() + lambdaInterfaces.size());
        for (int i = 0; i < parameterMetadatas.size(); i++) {
            ParameterMetadata parameterMetadata = parameterMetadatas.get(i);
            switch (parameterMetadata.getParameterType()) {
                case STATE:
                    block.append(scope.getThis().getField(stateField.get(stateIndex)));
                    stateIndex++;
                    break;
                case BLOCK_INDEX:
                    block.getVariable(position);
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    block.getVariable(parameterVariables.get(inputChannel));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    BytecodeBlock getBlockBytecode = new BytecodeBlock()
                            .getVariable(parameterVariables.get(inputChannel));
                    pushStackType(scope, block, parameterMetadata.getSqlType(), getBlockBytecode, parameters[i], callSiteBinder);
                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterMetadata.getParameterType());
            }
        }
        for (int i = 0; i < lambdaInterfaces.size(); i++) {
            block.append(scope.getThis().getField(lambdaProviderFields.get(i))
                    .invoke("getLambda", Object.class)
                    .cast(lambdaInterfaces.get(i)));
        }

        block.append(invoke(callSiteBinder.bind(inputFunction), "input"));
        return block;
    }

    // Assumes that there is a variable named 'position' in the block, which is the current index
    private static void pushStackType(Scope scope, BytecodeBlock block, Type sqlType, BytecodeBlock getBlockBytecode, Class<?> parameter, CallSiteBinder callSiteBinder)
    {
        Variable position = scope.getVariable("position");
        if (parameter == long.class) {
            block.comment("%s.getLong(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getLong", long.class, Block.class, int.class);
        }
        else if (parameter == double.class) {
            block.comment("%s.getDouble(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getDouble", double.class, Block.class, int.class);
        }
        else if (parameter == boolean.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getBoolean", boolean.class, Block.class, int.class);
        }
        else if (parameter == Slice.class) {
            block.comment("%s.getSlice(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getSlice", Slice.class, Block.class, int.class);
        }
        else {
            block.comment("%s.getObject(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockBytecode)
                    .append(position)
                    .invokeInterface(Type.class, "getObject", Object.class, Block.class, int.class);
        }
    }

    private static void generateAddIntermediateAsCombine(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            List<Class> lambdaInterfaces,
            List<FieldDefinition> lambdaProviderFields,
            MethodHandle combineFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        MethodDefinition method = declareAddIntermediate(definition, grouped);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        int stateCount = stateFieldAndDescriptors.size();
        List<Variable> scratchStates = new ArrayList<>();
        for (int i = 0; i < stateCount; i++) {
            Class<?> scratchStateClass = stateFieldAndDescriptors.get(i).getStateDescriptor().getFactory().getSingleStateClass();
            scratchStates.add(scope.declareVariable(scratchStateClass, "scratchState_" + i));
        }

        List<Variable> block;
        if (stateCount == 1) {
            block = ImmutableList.of(scope.getVariable("block"));
        }
        else {
            // ColumnarRow is used to get the column blocks represents each state, this allows to
            //  1. handle single state and multiple states in a unified way
            //  2. avoid the cost of constructing SingleRowBlock for each group
            Variable columnarRow = scope.declareVariable(ColumnarRow.class, "columnarRow");
            body.append(columnarRow.set(
                    invokeStatic(ColumnarRow.class, "toColumnarRow", ColumnarRow.class, scope.getVariable("block"))));

            block = new ArrayList<>();
            for (int i = 0; i < stateCount; i++) {
                Variable columnBlock = scope.declareVariable(Block.class, "columnBlock_" + i);
                body.append(columnBlock.set(
                        columnarRow.invoke("getField", Block.class, constantInt(i))));
                block.add(columnBlock);
            }
        }

        Variable position = scope.declareVariable(int.class, "position");
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateFactoryField = stateFieldAndDescriptors.get(i).getStateFactoryField();
            body.comment(format("scratchState_%s = stateFactory[%s].createSingleState();", i, i))
                    .append(thisVariable.getField(stateFactoryField))
                    .invokeInterface(AccumulatorStateFactory.class, "createSingleState", Object.class)
                    .checkCast(scratchStates.get(i).getType())
                    .putVariable(scratchStates.get(i));
        }

        List<FieldDefinition> stateFields = stateFieldAndDescriptors.stream()
                .map(StateFieldAndDescriptor::getStateField)
                .collect(toImmutableList());

        if (grouped) {
            generateEnsureCapacity(scope, stateFields, body);
        }

        BytecodeBlock loopBody = new BytecodeBlock();

        loopBody.comment("combine(state_0, state_1, ... scratchState_0, scratchState_1, ... lambda_0, lambda_1, ...)");
        for (FieldDefinition stateField : stateFields) {
            if (grouped) {
                Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
                loopBody.append(thisVariable.getField(stateField).invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
            }
            loopBody.append(thisVariable.getField(stateField));
        }
        for (int i = 0; i < stateCount; i++) {
            FieldDefinition stateSerializerField = stateFieldAndDescriptors.get(i).getStateSerializerField();
            loopBody.append(thisVariable.getField(stateSerializerField).invoke("deserialize", void.class, block.get(i), position, scratchStates.get(i).cast(Object.class)));
            loopBody.append(scratchStates.get(i));
        }
        for (int i = 0; i < lambdaInterfaces.size(); i++) {
            loopBody.append(scope.getThis().getField(lambdaProviderFields.get(i))
                    .invoke("getLambda", Object.class)
                    .cast(lambdaInterfaces.get(i)));
        }
        loopBody.append(invoke(callSiteBinder.bind(combineFunction), "combine"));

        if (grouped) {
            // skip rows with null group id
            IfStatement ifStatement = new IfStatement("if (!groupIdsBlock.isNull(position))")
                    .condition(not(scope.getVariable("groupIdsBlock").invoke("isNull", boolean.class, position)))
                    .ifTrue(loopBody);

            loopBody = new BytecodeBlock().append(ifStatement);
        }

        body.append(generateBlockNonNullPositionForLoop(scope, position, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIdsBlock(Scope scope, List<FieldDefinition> stateFields, BytecodeBlock block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        Variable position = scope.getVariable("position");
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = scope.getThis().getField(stateField);
            block.append(state.invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
        }
    }

    private static void generateEnsureCapacity(Scope scope, List<FieldDefinition> stateFields, BytecodeBlock block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = scope.getThis().getField(stateField);
            block.append(state.invoke("ensureCapacity", void.class, groupIdsBlock.invoke("getGroupCount", long.class)));
        }
    }

    private static MethodDefinition declareAddIntermediate(ClassDefinition definition, boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("block", Block.class));

        return definition.declareMethod(
                a(PUBLIC),
                "addIntermediate",
                type(void.class),
                parameters.build());
    }

    // Generates a for-loop with a local variable named "position" defined, with the current position in the block,
    // loopBody will only be executed for non-null positions in the Block
    private static BytecodeBlock generateBlockNonNullPositionForLoop(Scope scope, Variable positionVariable, BytecodeBlock loopBody)
    {
        Variable rowsVariable = scope.declareVariable(int.class, "rows");
        Variable blockVariable = scope.getVariable("block");

        BytecodeBlock block = new BytecodeBlock()
                .append(blockVariable)
                .invokeInterface(Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatement ifStatement = new IfStatement("if(!block.isNull(position))")
                .condition(new BytecodeBlock()
                        .append(blockVariable)
                        .append(positionVariable)
                        .invokeInterface(Block.class, "isNull", boolean.class, int.class))
                .ifFalse(loopBody);

        block.append(new ForLoop()
                .initialize(positionVariable.set(constantInt(0)))
                .condition(new BytecodeBlock()
                        .append(positionVariable)
                        .append(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new BytecodeBlock().incrementVariable(positionVariable, (byte) 1))
                .body(ifStatement));

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateIntermediate", type(void.class), groupId, out);

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                    .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                    .ret();
        }
        else {
            Variable rowBuilder = method.getScope().declareVariable(BlockBuilder.class, "rowBuilder");
            body.append(rowBuilder.set(out.invoke("beginBlockEntry", BlockBuilder.class)));

            for (int i = 0; i < stateFieldAndDescriptors.size(); i++) {
                BytecodeExpression stateSerializer = thisVariable.getField(stateFieldAndDescriptors.get(i).getStateSerializerField());
                BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptors.get(i).getStateField());

                body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                        .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), rowBuilder));
            }
            body.append(out.invoke("closeEntry", BlockBuilder.class).pop())
                    .ret();
        }
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, List<StateFieldAndDescriptor> stateFieldAndDescriptors)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                out);

        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        if (stateFieldAndDescriptors.size() == 1) {
            BytecodeExpression stateSerializer = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateSerializerField());
            BytecodeExpression state = thisVariable.getField(getOnlyElement(stateFieldAndDescriptors).getStateField());

            body.append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                    .ret();
        }
        else {
            Variable rowBuilder = method.getScope().declareVariable(BlockBuilder.class, "rowBuilder");
            body.append(rowBuilder.set(out.invoke("beginBlockEntry", BlockBuilder.class)));

            for (int i = 0; i < stateFieldAndDescriptors.size(); i++) {
                BytecodeExpression stateSerializer = thisVariable.getField(stateFieldAndDescriptors.get(i).getStateSerializerField());
                BytecodeExpression state = thisVariable.getField(stateFieldAndDescriptors.get(i).getStateField());
                body.append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), rowBuilder));
            }
            body.append(out.invoke("closeEntry", BlockBuilder.class).pop())
                    .ret();
        }
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateFinal", type(void.class), groupId, out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            List<FieldDefinition> stateFields,
            MethodHandle outputFunction,
            CallSiteBinder callSiteBinder)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                out);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        List<BytecodeExpression> states = new ArrayList<>();

        for (FieldDefinition stateField : stateFields) {
            BytecodeExpression state = thisVariable.getField(stateField);
            states.add(state);
        }

        body.comment("output(state_0, state_1, ..., out)");
        states.forEach(body::append);
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generatePrepareFinal(ClassDefinition definition)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "prepareFinal",
                type(void.class));
        method.getBody().ret();
    }

    private static void generateConstructor(
            ClassDefinition definition,
            List<StateFieldAndDescriptor> stateFieldAndDescriptors,
            FieldDefinition inputChannelsField,
            FieldDefinition maskChannelField,
            List<FieldDefinition> lambdaProviderFields,
            boolean grouped)
    {
        Parameter stateDescriptors = arg("stateDescriptors", type(List.class, AccumulatorStateDescriptor.class));
        Parameter inputChannels = arg("inputChannels", type(List.class, Integer.class));
        Parameter maskChannel = arg("maskChannel", type(Optional.class, Integer.class));
        Parameter lambdaProviders = arg("lambdaProviders", type(List.class, LambdaProvider.class));
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                stateDescriptors,
                inputChannels,
                maskChannel,
                lambdaProviders);

        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        for (int i = 0; i < stateFieldAndDescriptors.size(); i++) {
            body.append(thisVariable.setField(
                    stateFieldAndDescriptors.get(i).getStateSerializerField(),
                    stateDescriptors.invoke("get", Object.class, constantInt(i))
                            .cast(AccumulatorStateDescriptor.class)
                            .invoke("getSerializer", AccumulatorStateSerializer.class)));
            body.append(thisVariable.setField(
                    stateFieldAndDescriptors.get(i).getStateFactoryField(),
                    stateDescriptors.invoke("get", Object.class, constantInt(i))
                            .cast(AccumulatorStateDescriptor.class)
                            .invoke("getFactory", AccumulatorStateFactory.class)));
        }
        for (int i = 0; i < lambdaProviderFields.size(); i++) {
            body.append(thisVariable.setField(
                    lambdaProviderFields.get(i),
                    lambdaProviders.invoke("get", Object.class, constantInt(i))
                            .cast(LambdaProvider.class)));
        }

        body.append(thisVariable.setField(inputChannelsField, generateRequireNotNull(inputChannels)));
        body.append(thisVariable.setField(maskChannelField, generateRequireNotNull(maskChannel)));

        String createState;
        if (grouped) {
            createState = "createGroupedState";
        }
        else {
            createState = "createSingleState";
        }

        for (StateFieldAndDescriptor stateFieldAndDescriptor : stateFieldAndDescriptors) {
            FieldDefinition stateField = stateFieldAndDescriptor.getStateField();
            BytecodeExpression stateFactory = thisVariable.getField(stateFieldAndDescriptor.getStateFactoryField());

            body.append(thisVariable.setField(stateField, stateFactory.invoke(createState, Object.class).cast(stateField.getType())));
        }
        body.ret();
    }

    private static BytecodeExpression generateRequireNotNull(Variable variable)
    {
        return invokeStatic(Objects.class, "requireNonNull", Object.class, variable.cast(Object.class), constantString(variable.getName() + " is null"))
                .cast(variable.getType());
    }

    private static class StateFieldAndDescriptor
    {
        private final FieldDefinition stateSerializerField;
        private final FieldDefinition stateFactoryField;
        private final FieldDefinition stateField;

        private final AccumulatorStateDescriptor stateDescriptor;

        public StateFieldAndDescriptor(FieldDefinition stateSerializerField, FieldDefinition stateFactoryField, FieldDefinition stateField, AccumulatorStateDescriptor stateDescriptor)
        {
            this.stateSerializerField = requireNonNull(stateSerializerField, "stateSerializerField is null");
            this.stateFactoryField = requireNonNull(stateFactoryField, "stateFactoryField is null");
            this.stateField = requireNonNull(stateField, "stateField is null");
            this.stateDescriptor = requireNonNull(stateDescriptor, "stateDescriptor is null");
        }

        public FieldDefinition getStateSerializerField()
        {
            return stateSerializerField;
        }

        public FieldDefinition getStateFactoryField()
        {
            return stateFactoryField;
        }

        public FieldDefinition getStateField()
        {
            return stateField;
        }

        public AccumulatorStateDescriptor getStateDescriptor()
        {
            return stateDescriptor;
        }
    }
}
