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
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerOperations;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.countInputChannels;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AccumulatorCompiler
{
    public GenericAccumulatorFactoryBinder generateAccumulatorFactoryBinder(AggregationMetadata metadata, DynamicClassLoader classLoader)
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
                metadata.getStateSerializer(),
                metadata.getStateFactory(),
                accumulatorClass,
                groupedAccumulatorClass,
                metadata.isApproximate());
    }

    private static <T> Class<? extends T> generateAccumulatorClass(
            Class<T> accumulatorInterface,
            AggregationMetadata metadata,
            DynamicClassLoader classLoader)
    {
        boolean grouped = accumulatorInterface == GroupedAccumulator.class;
        boolean approximate = metadata.isApproximate();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(metadata.getName() + accumulatorInterface.getSimpleName()),
                type(Object.class),
                type(accumulatorInterface));

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        AccumulatorStateSerializer<?> stateSerializer = metadata.getStateSerializer();
        AccumulatorStateFactory<?> stateFactory = metadata.getStateFactory();

        FieldDefinition stateSerializerField = definition.declareField(a(PRIVATE, FINAL), "stateSerializer", AccumulatorStateSerializer.class);
        FieldDefinition stateFactoryField = definition.declareField(a(PRIVATE, FINAL), "stateFactory", AccumulatorStateFactory.class);
        FieldDefinition inputChannelsField = definition.declareField(a(PRIVATE, FINAL), "inputChannels", type(List.class, Integer.class));
        FieldDefinition maskChannelField = definition.declareField(a(PRIVATE, FINAL), "maskChannel", type(Optional.class, Integer.class));
        FieldDefinition sampleWeightChannelField = null;
        FieldDefinition confidenceField = null;
        if (approximate) {
            sampleWeightChannelField = definition.declareField(a(PRIVATE, FINAL), "sampleWeightChannel", type(Optional.class, Integer.class));
            confidenceField = definition.declareField(a(PRIVATE, FINAL), "confidence", double.class);
        }
        FieldDefinition stateField = definition.declareField(a(PRIVATE, FINAL), "state", grouped ? stateFactory.getGroupedStateClass() : stateFactory.getSingleStateClass());

        // Generate constructor
        generateConstructor(
                definition,
                stateSerializerField,
                stateFactoryField,
                inputChannelsField,
                maskChannelField,
                sampleWeightChannelField,
                confidenceField,
                stateField,
                grouped);

        // Generate methods
        generateAddInput(definition, stateField, inputChannelsField, maskChannelField, sampleWeightChannelField, metadata.getInputMetadata(), metadata.getInputFunction(), callSiteBinder, grouped);
        generateGetEstimatedSize(definition, stateField);
        generateGetIntermediateType(definition, callSiteBinder, stateSerializer.getSerializedType());
        generateGetFinalType(definition, callSiteBinder, metadata.getOutputType());

        if (metadata.getIntermediateInputFunction() == null) {
            generateAddIntermediateAsCombine(definition, stateField, stateSerializerField, stateFactoryField, metadata.getCombineFunction(), stateFactory.getSingleStateClass(), callSiteBinder, grouped);
        }
        else {
            generateAddIntermediateAsIntermediateInput(definition, stateField, metadata.getIntermediateInputMetadata(), metadata.getIntermediateInputFunction(), callSiteBinder, grouped);
        }

        if (grouped) {
            generateGroupedEvaluateIntermediate(definition, stateSerializerField, stateField);
        }
        else {
            generateEvaluateIntermediate(definition, stateSerializerField, stateField);
        }

        if (grouped) {
            generateGroupedEvaluateFinal(definition, confidenceField, stateSerializerField, stateField, metadata.getOutputFunction(), metadata.isApproximate(), callSiteBinder);
        }
        else {
            generateEvaluateFinal(definition, confidenceField, stateSerializerField, stateField, metadata.getOutputFunction(), metadata.isApproximate(), callSiteBinder);
        }

        return defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
    }

    private static MethodDefinition generateGetIntermediateType(ClassDefinition definition, CallSiteBinder callSiteBinder, Type type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getIntermediateType", type(Type.class));

        methodDefinition.getBody()
                .append(constantType(callSiteBinder, type))
                .retObject();

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

    private static void generateGetEstimatedSize(ClassDefinition definition, FieldDefinition stateField)
    {
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        ByteCodeExpression state = method.getThis().getField(stateField);
        method.getBody()
                .append(state.invoke("getEstimatedSize", long.class).ret());
    }

    private static void generateAddInput(
            ClassDefinition definition,
            FieldDefinition stateField,
            FieldDefinition inputChannelsField,
            FieldDefinition maskChannelField,
            @Nullable FieldDefinition sampleWeightChannelField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("page", Page.class));

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = context.getThis();
        Variable page = context.getVariable("page");
        Block body = method.getBody();

        if (grouped) {
            generateEnsureCapacity(context, stateField, body);
        }

        List<Variable> parameterVariables = new ArrayList<>();
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            parameterVariables.add(context.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i));
        }
        Variable masksBlock = context.declareVariable(com.facebook.presto.spi.block.Block.class, "masksBlock");
        Variable sampleWeightsBlock = null;
        if (sampleWeightChannelField != null) {
            sampleWeightsBlock = context.declareVariable(com.facebook.presto.spi.block.Block.class, "sampleWeightsBlock");
        }
        body.comment("masksBlock = maskChannel.map(page.blockGetter()).orElse(null);")
                .append(thisVariable.getField(maskChannelField))
                .append(page)
                .invokeStatic(type(AggregationUtils.class), "pageBlockGetter", type(Function.class, Integer.class, com.facebook.presto.spi.block.Block.class), type(Page.class))
                .invokeVirtual(Optional.class, "map", Optional.class, Function.class)
                .pushNull()
                .invokeVirtual(Optional.class, "orElse", Object.class, Object.class)
                .checkCast(com.facebook.presto.spi.block.Block.class)
                .putVariable(masksBlock);

        if (sampleWeightChannelField != null) {
            body.comment("sampleWeightsBlock = sampleWeightChannel.map(page.blockGetter()).get();")
                    .append(thisVariable.getField(sampleWeightChannelField))
                    .append(page)
                    .invokeStatic(type(AggregationUtils.class), "pageBlockGetter", type(Function.class, Integer.class, com.facebook.presto.spi.block.Block.class), type(Page.class))
                    .invokeVirtual(Optional.class, "map", Optional.class, Function.class)
                    .invokeVirtual(Optional.class, "get", Object.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .putVariable(sampleWeightsBlock);
        }

        // Get all parameter blocks
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            body.comment("%s = page.getBlock(inputChannels.get(%d));", parameterVariables.get(i).getName(), i)
                    .append(page)
                    .append(thisVariable.getField(inputChannelsField))
                    .push(i)
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(Integer.class)
                    .invokeVirtual(Integer.class, "intValue", int.class)
                    .invokeVirtual(Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .putVariable(parameterVariables.get(i));
        }
        Block block = generateInputForLoop(stateField, parameterMetadatas, inputFunction, context, parameterVariables, masksBlock, sampleWeightsBlock, callSiteBinder, grouped);

        body.append(block);
        body.ret();
    }

    private static Block generateInputForLoop(
            FieldDefinition stateField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            CompilerContext context,
            List<Variable> parameterVariables,
            Variable masksBlock,
            @Nullable Variable sampleWeightsBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable page = context.getVariable("page");
        Variable positionVariable = context.declareVariable(int.class, "position");
        Variable sampleWeightVariable = null;
        if (sampleWeightsBlock != null) {
            sampleWeightVariable = context.declareVariable(long.class, "sampleWeight");
        }
        Variable rowsVariable = context.declareVariable(int.class, "rows");

        Block block = new Block(context)
                .append(page)
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);
        if (sampleWeightVariable != null) {
            block.initializeVariable(sampleWeightVariable);
        }

        ByteCodeNode loopBody = generateInvokeInputFunction(context, stateField, positionVariable, sampleWeightVariable, parameterVariables, parameterMetadatas, inputFunction, callSiteBinder, grouped);

        //  Wrap with null checks
        List<Boolean> nullable = new ArrayList<>();
        for (ParameterMetadata metadata : parameterMetadatas) {
            if (metadata.getParameterType() == INPUT_CHANNEL) {
                nullable.add(false);
            }
            else if (metadata.getParameterType() == NULLABLE_INPUT_CHANNEL) {
                nullable.add(true);
            }
        }
        checkState(nullable.size() == parameterVariables.size(), "Number of parameters does not match");
        for (int i = 0; i < parameterVariables.size(); i++) {
            if (!nullable.get(i)) {
                Variable variableDefinition = parameterVariables.get(i);
                loopBody = new IfStatement("if(!%s.isNull(position))", variableDefinition.getName())
                        .condition(new Block(context)
                                .getVariable(variableDefinition)
                                .getVariable(positionVariable)
                                .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                        .ifFalse(loopBody);
            }
        }

        // Check that sample weight is > 0 (also checks the mask)
        if (sampleWeightVariable != null) {
            loopBody = generateComputeSampleWeightAndCheckGreaterThanZero(context, loopBody, sampleWeightVariable, masksBlock, sampleWeightsBlock, positionVariable);
        }
        // Otherwise just check the mask
        else {
            loopBody = new IfStatement("if(testMask(%s, position))", masksBlock.getName())
                    .condition(new Block(context)
                            .getVariable(masksBlock)
                            .getVariable(positionVariable)
                            .invokeStatic(CompilerOperations.class, "testMask", boolean.class, com.facebook.presto.spi.block.Block.class, int.class))
                    .ifTrue(loopBody);
        }

        block.append(new ForLoop()
                .initialize(new Block(context).putVariable(positionVariable, 0))
                .condition(new Block(context)
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(context).incrementVariable(positionVariable, (byte) 1))
                .body(loopBody));

        return block;
    }

    private static ByteCodeNode generateComputeSampleWeightAndCheckGreaterThanZero(CompilerContext context, ByteCodeNode body, Variable sampleWeight, Variable masks, Variable sampleWeights, Variable position)
    {
        Block block = new Block(context)
                .comment("sampleWeight = computeSampleWeight(masks, sampleWeights, position);")
                .getVariable(masks)
                .getVariable(sampleWeights)
                .getVariable(position)
                .invokeStatic(ApproximateUtils.class, "computeSampleWeight", long.class, com.facebook.presto.spi.block.Block.class, com.facebook.presto.spi.block.Block.class, int.class)
                .putVariable(sampleWeight);

        block.append(new IfStatement("if(sampleWeight > 0)")
                .condition(new Block(context)
                        .getVariable(sampleWeight)
                        .invokeStatic(CompilerOperations.class, "longGreaterThanZero", boolean.class, long.class))
                .ifTrue(body)
                .ifFalse(NOP));

        return block;
    }

    private static Block generateInvokeInputFunction(
            CompilerContext context,
            FieldDefinition stateField,
            Variable position,
            @Nullable Variable sampleWeight,
            List<Variable> parameterVariables,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        Block block = new Block(context);

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(context, stateField, block);
        }

        block.comment("Call input function with unpacked Block arguments");

        Class<?>[] parameters = inputFunction.type().parameterArray();
        int inputChannel = 0;
        for (int i = 0; i < parameters.length; i++) {
            ParameterMetadata parameterMetadata = parameterMetadatas.get(i);
            switch (parameterMetadata.getParameterType()) {
                case STATE:
                    block.append(context.getThis().getField(stateField));
                    break;
                case BLOCK_INDEX:
                    block.getVariable(position);
                    break;
                case SAMPLE_WEIGHT:
                    checkNotNull(sampleWeight, "sampleWeight is null");
                    block.getVariable(sampleWeight);
                    break;
                case NULLABLE_INPUT_CHANNEL:
                    block.getVariable(parameterVariables.get(inputChannel));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    Block getBlockByteCode = new Block(context)
                            .getVariable(parameterVariables.get(inputChannel));
                    pushStackType(context, block, parameterMetadata.getSqlType(), getBlockByteCode, parameters[i], callSiteBinder);
                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterMetadata.getParameterType());
            }
        }

        block.append(invoke(callSiteBinder.bind(inputFunction), "input"));
        return block;
    }

    // Assumes that there is a variable named 'position' in the block, which is the current index
    private static void pushStackType(CompilerContext context, Block block, Type sqlType, Block getBlockByteCode, Class<?> parameter, CallSiteBinder callSiteBinder)
    {
        Variable position = context.getVariable("position");
        if (parameter == com.facebook.presto.spi.block.Block.class) {
            block.append(getBlockByteCode);
        }
        else if (parameter == long.class) {
            block.comment("%s.getLong(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getLong", long.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == double.class) {
            block.comment("%s.getDouble(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getDouble", double.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == boolean.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getBoolean", boolean.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == Slice.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getSlice", Slice.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else {
            throw new IllegalArgumentException("Unsupported parameter type: " + parameter.getSimpleName());
        }
    }

    private static void generateAddIntermediateAsCombine(
            ClassDefinition definition,
            FieldDefinition stateField,
            FieldDefinition stateSerializerField,
            FieldDefinition stateFactoryField,
            MethodHandle combineFunction,
            Class<?> singleStateClass,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        CompilerContext context = new CompilerContext();

        Block body = declareAddIntermediate(definition, grouped, context);

        Variable block = context.getVariable("block");
        Variable scratchState = context.declareVariable(singleStateClass, "scratchState");
        Variable position = context.declareVariable(int.class, "position");

        body.comment("scratchState = stateFactory.createSingleState();")
                .append(context.getThis().getField(stateFactoryField))
                .invokeInterface(AccumulatorStateFactory.class, "createSingleState", Object.class)
                .checkCast(scratchState.getType())
                .putVariable(scratchState);

        if (grouped) {
            generateEnsureCapacity(context, stateField, body);
        }

        Block loopBody = new Block(context);

        if (grouped) {
            Variable groupIdsBlock = context.getVariable("groupIdsBlock");
            loopBody.append(context.getThis().getField(stateField).invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
        }

        loopBody.append(context.getThis().getField(stateSerializerField).invoke("deserialize", void.class, block, position, scratchState.cast(Object.class)));

        loopBody.comment("combine(state, scratchState)")
                .append(context.getThis().getField(stateField))
                .append(scratchState)
                .append(invoke(callSiteBinder.bind(combineFunction), "combine"));

        body.append(generateBlockNonNullPositionForLoop(context, position, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIdsBlock(CompilerContext context, FieldDefinition stateField, Block block)
    {
        Variable groupIdsBlock = context.getVariable("groupIdsBlock");
        Variable position = context.getVariable("position");
        ByteCodeExpression state = context.getThis().getField(stateField);
        block.append(state.invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
    }

    private static void generateEnsureCapacity(CompilerContext context, FieldDefinition stateField, Block block)
    {
        Variable groupIdsBlock = context.getVariable("groupIdsBlock");
        ByteCodeExpression state = context.getThis().getField(stateField);
        block.append(state.invoke("ensureCapacity", void.class, groupIdsBlock.invoke("getGroupCount", long.class)));
    }

    private static Block declareAddIntermediate(ClassDefinition definition, boolean grouped, CompilerContext context)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("block", com.facebook.presto.spi.block.Block.class));

        return definition.declareMethod(
                context,
                a(PUBLIC),
                "addIntermediate",
                type(void.class),
                parameters.build())
                .getBody();
    }

    private static void generateAddIntermediateAsIntermediateInput(
            ClassDefinition definition,
            FieldDefinition stateField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle intermediateInputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        CompilerContext context = new CompilerContext();

        Block body = declareAddIntermediate(definition, grouped, context);

        if (grouped) {
            generateEnsureCapacity(context, stateField, body);
        }

        Variable positionVariable = context.declareVariable(int.class, "position");

        Block loopBody = generateInvokeInputFunction(context, stateField, positionVariable, null, ImmutableList.of(context.getVariable("block")), parameterMetadatas, intermediateInputFunction, callSiteBinder, grouped);

        body.append(generateBlockNonNullPositionForLoop(context, positionVariable, loopBody))
                .ret();
    }

    // Generates a for-loop with a local variable named "position" defined, with the current position in the block,
    // loopBody will only be executed for non-null positions in the Block
    private static Block generateBlockNonNullPositionForLoop(CompilerContext context, Variable positionVariable, Block loopBody)
    {
        Variable rowsVariable = context.declareVariable(int.class, "rows");
        Variable blockVariable = context.getVariable("block");

        Block block = new Block(context)
                .append(blockVariable)
                .invokeInterface(com.facebook.presto.spi.block.Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatement ifStatement = new IfStatement("if(!block.isNull(position))")
                .condition(new Block(context)
                        .append(blockVariable)
                        .append(positionVariable)
                        .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                .ifFalse(loopBody);

        block.append(new ForLoop()
                .initialize(new Block(context).putVariable(positionVariable, 0))
                .condition(new Block(context)
                        .append(positionVariable)
                        .append(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(context).incrementVariable(positionVariable, (byte) 1))
                .body(ifStatement));

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                arg("groupId", int.class),
                arg("out", BlockBuilder.class));

        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = method.getThis();
        Variable groupId = context.getVariable("groupId");
        Variable out = context.getVariable("out");

        ByteCodeExpression state = thisVariable.getField(stateField);
        ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);

        method.getBody()
                .append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                .ret();
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                arg("out", BlockBuilder.class));

        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = method.getThis();
        Variable out = context.getVariable("out");
        ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);
        ByteCodeExpression state = thisVariable.getField(stateField);

        method.getBody()
                .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                .ret();
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            FieldDefinition confidenceField,
            FieldDefinition stateSerializerField,
            FieldDefinition stateField,
            @Nullable MethodHandle outputFunction,
            boolean approximate,
            CallSiteBinder callSiteBinder)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                arg("groupId", int.class),
                arg("out", BlockBuilder.class));

        Block body = method.getBody();
        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = method.getThis();
        Variable groupId = context.getVariable("groupId");
        Variable out = context.getVariable("out");

        ByteCodeExpression state = thisVariable.getField(stateField);

        body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));

        if (outputFunction != null) {
            body.comment("output(state, out)");
            body.append(state);
            if (approximate) {
                checkNotNull(confidenceField, "confidenceField is null");
                body.append(thisVariable.getField(confidenceField));
            }
            body.append(out);
            body.append(invoke(callSiteBinder.bind(outputFunction), "output"));
        }
        else {
            checkArgument(!approximate, "Approximate aggregations must specify an output function");
            ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);
            body.append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out));
        }
        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            FieldDefinition confidenceField,
            FieldDefinition stateSerializerField,
            FieldDefinition stateField,
            @Nullable
            MethodHandle outputFunction,
            boolean approximate,
            CallSiteBinder callSiteBinder)
    {
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                arg("out", BlockBuilder.class));

        Block body = method.getBody();
        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = method.getThis();
        Variable out = context.getVariable("out");

        ByteCodeExpression state = thisVariable.getField(stateField);

        if (outputFunction != null) {
            body.comment("output(state, out)");
            body.append(state);
            if (approximate) {
                checkNotNull(confidenceField, "confidenceField is null");
                body.append(thisVariable.getField(confidenceField));
            }
            body.append(out);
            body.append(invoke(callSiteBinder.bind(outputFunction), "output"));
        }
        else {
            checkArgument(!approximate, "Approximate aggregations must specify an output function");
            ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);
            body.append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out));
        }
        body.ret();
    }

    private static void generateConstructor(
            ClassDefinition definition,
            FieldDefinition stateSerializerField,
            FieldDefinition stateFactoryField,
            FieldDefinition inputChannelsField,
            FieldDefinition maskChannelField,
            @Nullable FieldDefinition sampleWeightChannelField,
            @Nullable FieldDefinition confidenceField,
            FieldDefinition stateField,
            boolean grouped)
    {
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                arg("stateSerializer", AccumulatorStateSerializer.class),
                arg("stateFactory", AccumulatorStateFactory.class),
                arg("inputChannels", type(List.class, Integer.class)),
                arg("maskChannel", type(Optional.class, Integer.class)),
                arg("sampleWeightChannel", type(Optional.class, Integer.class)),
                arg("confidence", double.class));

        Block body = method.getBody();
        CompilerContext context = method.getCompilerContext();
        Variable thisVariable = method.getThis();
        Variable stateSerializer = context.getVariable("stateSerializer");
        Variable stateFactory = context.getVariable("stateFactory");
        Variable inputChannels = context.getVariable("inputChannels");
        Variable maskChannel = context.getVariable("maskChannel");
        Variable sampleWeightChannel = context.getVariable("sampleWeightChannel");
        Variable confidence = context.getVariable("confidence");

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        body.append(thisVariable.setField(stateSerializerField, generateRequireNotNull(stateSerializer)));
        body.append(thisVariable.setField(stateFactoryField, generateRequireNotNull(stateFactory)));
        body.append(thisVariable.setField(inputChannelsField, generateRequireNotNull(inputChannels)));
        body.append(thisVariable.setField(maskChannelField, generateRequireNotNull(maskChannel)));

        if (sampleWeightChannelField != null) {
            body.append(thisVariable.setField(sampleWeightChannelField, generateRequireNotNull(sampleWeightChannel)));
        }

        String createState;
        if (grouped) {
            createState = "createGroupedState";
        }
        else {
            createState = "createSingleState";
        }

        if (confidenceField != null) {
            body.append(thisVariable.setField(confidenceField, confidence));
        }

        body.append(thisVariable.setField(stateField, stateFactory.invoke(createState, Object.class).cast(stateField.getType())));
        body.ret();
    }

    private static ByteCodeExpression generateRequireNotNull(Variable variable)
    {
        return invokeStatic(Objects.class, "requireNonNull", Object.class, variable.cast(Object.class), constantString(variable.getName() + " is null"))
                .cast(variable.getType());
    }
}
