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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerOperations;
import com.facebook.presto.sql.gen.SqlTypeByteCodeExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import static com.facebook.presto.byteCode.control.IfStatement.ifStatementBuilder;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.countInputChannels;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
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
        MethodDefinition getIntermediateType = generateGetIntermediateType(definition, callSiteBinder, stateSerializer.getSerializedType());
        MethodDefinition getFinalType = generateGetFinalType(definition, callSiteBinder, metadata.getOutputType());

        if (metadata.getIntermediateInputFunction() == null) {
            generateAddIntermediateAsCombine(definition, stateField, stateSerializerField, stateFactoryField, metadata.getCombineFunction(), stateFactory.getSingleStateClass(), grouped);
        }
        else {
            generateAddIntermediateAsIntermediateInput(definition, stateField, metadata.getIntermediateInputMetadata(), metadata.getIntermediateInputFunction(), callSiteBinder, grouped);
        }

        if (grouped) {
            generateGroupedEvaluateIntermediate(definition, stateSerializerField, stateField);
        }
        else {
            generateEvaluateIntermediate(definition, getIntermediateType, stateSerializerField, stateField);
        }

        if (grouped) {
            generateGroupedEvaluateFinal(definition, confidenceField, stateSerializerField, stateField, metadata.getOutputFunction(), metadata.isApproximate());
        }
        else {
            generateEvaluateFinal(definition, getFinalType, confidenceField, stateSerializerField, stateField, metadata.getOutputFunction(), metadata.isApproximate());
        }

        return defineClass(definition, accumulatorInterface, callSiteBinder.getBindings(), classLoader);
    }

    private static MethodDefinition generateGetIntermediateType(ClassDefinition definition, CallSiteBinder callSiteBinder, Type type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getIntermediateType", type(Type.class));

        methodDefinition.getBody()
                .append(constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, type))
                .retObject();

        return methodDefinition;
    }

    private static MethodDefinition generateGetFinalType(ClassDefinition definition, CallSiteBinder callSiteBinder, Type type)
    {
        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC), "getFinalType", type(Type.class));

        methodDefinition.getBody()
                .append(constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, type))
                .retObject();

        return methodDefinition;
    }

    private static void generateGetEstimatedSize(ClassDefinition definition, FieldDefinition stateField)
    {
        definition.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class))
                .getBody()
                .pushThis()
                .getField(stateField)
                .invokeVirtual(stateField.getType(), "getEstimatedSize", type(long.class))
                .retLong();
    }

    private static void generateAddInput(
            ClassDefinition definition,
            FieldDefinition stateField,
            FieldDefinition inputChannelsField,
            FieldDefinition maskChannelField,
            @Nullable FieldDefinition sampleWeightChannelField,
            List<ParameterMetadata> parameterMetadatas,
            Method inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        CompilerContext context = new CompilerContext();

        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("page", Page.class));

        Block body = definition.declareMethod(context, a(PUBLIC), "addInput", type(void.class), parameters.build())
                .getBody();

        if (grouped) {
            generateEnsureCapacity(stateField, body);
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
                .pushThis()
                .getField(maskChannelField)
                .getVariable("page")
                .invokeStatic(type(AggregationUtils.class), "pageBlockGetter", type(Function.class, Integer.class, com.facebook.presto.spi.block.Block.class), type(Page.class))
                .invokeVirtual(Optional.class, "map", Optional.class, Function.class)
                .pushNull()
                .invokeVirtual(Optional.class, "orElse", Object.class, Object.class)
                .checkCast(com.facebook.presto.spi.block.Block.class)
                .putVariable(masksBlock);

        if (sampleWeightChannelField != null) {
            body.comment("sampleWeightsBlock = sampleWeightChannel.map(page.blockGetter()).get();")
                    .pushThis()
                    .getField(sampleWeightChannelField)
                    .getVariable("page")
                    .invokeStatic(type(AggregationUtils.class), "pageBlockGetter", type(Function.class, Integer.class, com.facebook.presto.spi.block.Block.class), type(Page.class))
                    .invokeVirtual(Optional.class, "map", Optional.class, Function.class)
                    .invokeVirtual(Optional.class, "get", Object.class)
                    .checkCast(com.facebook.presto.spi.block.Block.class)
                    .putVariable(sampleWeightsBlock);
        }

        // Get all parameter blocks
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            body.comment("%s = page.getBlock(inputChannels.get(%d));", parameterVariables.get(i).getName(), i)
                    .getVariable("page")
                    .pushThis()
                    .getField(inputChannelsField)
                    .push(i)
                    .invokeInterface(List.class, "get", Object.class, int.class)
                    .checkCast(Integer.class)
                    .invokeVirtual(Integer.class, "intValue", int.class)
                    .invokeVirtual(Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .putVariable(parameterVariables.get(i));
        }
        Block block = generateInputForLoop(stateField, parameterMetadatas, inputFunction, context, parameterVariables, masksBlock, sampleWeightsBlock, callSiteBinder, grouped);

        body.append(block)
                .ret();
    }

    private static Block generateInputForLoop(
            FieldDefinition stateField,
            List<ParameterMetadata> parameterMetadatas,
            Method inputFunction,
            CompilerContext context,
            List<Variable> parameterVariables,
            Variable masksBlock,
            @Nullable Variable sampleWeightsBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable positionVariable = context.declareVariable(int.class, "position");
        Variable sampleWeightVariable = null;
        if (sampleWeightsBlock != null) {
            sampleWeightVariable = context.declareVariable(long.class, "sampleWeight");
        }
        Variable rowsVariable = context.declareVariable(int.class, "rows");

        Block block = new Block(context)
                .getVariable("page")
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);
        if (sampleWeightVariable != null) {
            block.initializeVariable(sampleWeightVariable);
        }

        Block loopBody = generateInvokeInputFunction(context, stateField, positionVariable, sampleWeightVariable, parameterVariables, parameterMetadatas, inputFunction, callSiteBinder, grouped);

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
                IfStatementBuilder builder = ifStatementBuilder(context);
                Variable variableDefinition = parameterVariables.get(i);
                builder.comment("if(!%s.isNull(position))", variableDefinition.getName())
                        .condition(new Block(context)
                                .getVariable(variableDefinition)
                                .getVariable(positionVariable)
                                .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                        .ifTrue(NOP)
                        .ifFalse(loopBody);
                loopBody = new Block(context).append(builder.build());
            }
        }

        // Check that sample weight is > 0 (also checks the mask)
        if (sampleWeightVariable != null) {
            loopBody = generateComputeSampleWeightAndCheckGreaterThanZero(context, loopBody, sampleWeightVariable, masksBlock, sampleWeightsBlock, positionVariable);
        }
        // Otherwise just check the mask
        else {
            IfStatementBuilder builder = ifStatementBuilder(context);
            builder.comment("if(testMask(%s, position))", masksBlock.getName())
                    .condition(new Block(context)
                            .getVariable(masksBlock)
                            .getVariable(positionVariable)
                            .invokeStatic(CompilerOperations.class, "testMask", boolean.class, com.facebook.presto.spi.block.Block.class, int.class))
                    .ifTrue(loopBody)
                    .ifFalse(NOP);
            loopBody = new Block(context).append(builder.build());
        }

        block.append(new ForLoop.ForLoopBuilder(context)
                .initialize(new Block(context).putVariable(positionVariable, 0))
                .condition(new Block(context)
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(context).incrementVariable(positionVariable, (byte) 1))
                .body(loopBody)
                .build());

        return block;
    }

    private static Block generateComputeSampleWeightAndCheckGreaterThanZero(CompilerContext context, Block body, Variable sampleWeight, Variable masks, Variable sampleWeights, Variable position)
    {
        Block block = new Block(context)
                .comment("sampleWeight = computeSampleWeight(masks, sampleWeights, position);")
                .getVariable(masks)
                .getVariable(sampleWeights)
                .getVariable(position)
                .invokeStatic(ApproximateUtils.class, "computeSampleWeight", long.class, com.facebook.presto.spi.block.Block.class, com.facebook.presto.spi.block.Block.class, int.class)
                .putVariable(sampleWeight);

        IfStatementBuilder builder = ifStatementBuilder(context);
        builder.comment("if(sampleWeight > 0)")
                .condition(new Block(context)
                        .getVariable(sampleWeight)
                        .invokeStatic(CompilerOperations.class, "longGreaterThanZero", boolean.class, long.class))
                .ifTrue(body)
                .ifFalse(NOP);

        return block.append(builder.build());
    }

    private static Block generateInvokeInputFunction(
            CompilerContext context,
            FieldDefinition stateField,
            Variable position,
            @Nullable Variable sampleWeight,
            List<Variable> parameterVariables,
            List<ParameterMetadata> parameterMetadatas,
            Method inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        Block block = new Block(context);

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(stateField, position, block);
        }

        block.comment("Call input function with unpacked Block arguments");

        Class<?>[] parameters = inputFunction.getParameterTypes();
        int inputChannel = 0;
        for (int i = 0; i < parameters.length; i++) {
            ParameterMetadata parameterMetadata = parameterMetadatas.get(i);
            switch (parameterMetadata.getParameterType()) {
                case STATE:
                    block.pushThis().getField(stateField);
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
                    pushStackType(block, parameterMetadata.getSqlType(), getBlockByteCode, parameters[i], callSiteBinder);
                    inputChannel++;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter type: " + parameterMetadata.getParameterType());
            }
        }

        block.invokeStatic(inputFunction);
        return block;
    }

    // Assumes that there is a variable named 'position' in the block, which is the current index
    private static void pushStackType(Block block, Type sqlType, Block getBlockByteCode, Class<?> parameter, CallSiteBinder callSiteBinder)
    {
        if (parameter == com.facebook.presto.spi.block.Block.class) {
            block.append(getBlockByteCode);
        }
        else if (parameter == long.class) {
            block.comment("%s.getLong(block, position)", sqlType.getTypeSignature())
                    .append(SqlTypeByteCodeExpression.constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .getVariable("position")
                    .invokeInterface(Type.class, "getLong", long.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == double.class) {
            block.comment("%s.getDouble(block, position)", sqlType.getTypeSignature())
                    .append(SqlTypeByteCodeExpression.constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .getVariable("position")
                    .invokeInterface(Type.class, "getDouble", double.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == boolean.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(SqlTypeByteCodeExpression.constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .getVariable("position")
                    .invokeInterface(Type.class, "getBoolean", boolean.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else if (parameter == Slice.class) {
            block.comment("%s.getBoolean(block, position)", sqlType.getTypeSignature())
                    .append(SqlTypeByteCodeExpression.constantType(new CompilerContext(BOOTSTRAP_METHOD), callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .getVariable("position")
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
            Method combineFunction,
            Class<?> singleStateClass,
            boolean grouped)
    {
        CompilerContext context = new CompilerContext();

        Block body = declareAddIntermediate(definition, grouped, context);

        Variable scratchStateVariable = context.declareVariable(singleStateClass, "scratchState");
        Variable positionVariable = context.declareVariable(int.class, "position");

        body.comment("scratchState = stateFactory.createSingleState();")
                .pushThis()
                .getField(stateFactoryField)
                .invokeInterface(AccumulatorStateFactory.class, "createSingleState", Object.class)
                .checkCast(scratchStateVariable.getType())
                .putVariable(scratchStateVariable);

        if (grouped) {
            generateEnsureCapacity(stateField, body);
        }

        Block loopBody = new Block(context);

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(stateField, positionVariable, loopBody);
        }

        loopBody.comment("stateSerializer.deserialize(block, position, scratchState)")
                .pushThis()
                .getField(stateSerializerField)
                .getVariable("block")
                .getVariable(positionVariable)
                .getVariable(scratchStateVariable)
                .invokeInterface(AccumulatorStateSerializer.class, "deserialize", void.class, com.facebook.presto.spi.block.Block.class, int.class, Object.class);

        loopBody.comment("combine(state, scratchState)")
                .pushThis()
                .getField(stateField)
                .getVariable("scratchState")
                .invokeStatic(combineFunction);

        body.append(generateBlockNonNullPositionForLoop(context, positionVariable, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIdsBlock(FieldDefinition stateField, Variable positionVariable, Block block)
    {
        block.comment("state.setGroupId(groupIdsBlock.getGroupId(position))")
                .pushThis()
                .getField(stateField)
                .getVariable("groupIdsBlock")
                .getVariable(positionVariable)
                .invokeVirtual(GroupByIdBlock.class, "getGroupId", long.class, int.class)
                .invokeVirtual(stateField.getType(), "setGroupId", type(void.class), type(long.class));
    }

    private static void generateEnsureCapacity(FieldDefinition stateField, Block block)
    {
        block.comment("state.ensureCapacity(groupIdsBlock.getGroupCount())")
                .pushThis()
                .getField(stateField)
                .getVariable("groupIdsBlock")
                .invokeVirtual(GroupByIdBlock.class, "getGroupCount", long.class)
                .invokeVirtual(stateField.getType(), "ensureCapacity", type(void.class), type(long.class));
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
            Method intermediateInputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        CompilerContext context = new CompilerContext();

        Block body = declareAddIntermediate(definition, grouped, context);

        if (grouped) {
            generateEnsureCapacity(stateField, body);
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

        Block block = new Block(context)
                .getVariable("block")
                .invokeInterface(com.facebook.presto.spi.block.Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatementBuilder builder = ifStatementBuilder(context);
        builder.comment("if(!block.isNull(position))")
                .condition(new Block(context)
                        .getVariable("block")
                        .getVariable(positionVariable)
                        .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                .ifTrue(NOP)
                .ifFalse(loopBody);

        block.append(new ForLoop.ForLoopBuilder(context)
                .initialize(new Block(context).putVariable(positionVariable, 0))
                .condition(new Block(context)
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(context).incrementVariable(positionVariable, (byte) 1))
                .body(builder.build())
                .build());

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                arg("groupId", int.class),
                arg("out", BlockBuilder.class))
                .getBody()
                .comment("state.setGroupId(groupId)")
                .pushThis()
                .getField(stateField)
                .getVariable("groupId")
                .intToLong()
                .invokeVirtual(stateField.getType(), "setGroupId", type(void.class), type(long.class))

                .comment("stateSerializer.serialize(state, out)")
                .pushThis()
                .getField(stateSerializerField)
                .pushThis()
                .getField(stateField)
                .getVariable("out")
                .invokeInterface(AccumulatorStateSerializer.class, "serialize", void.class, Object.class, BlockBuilder.class)
                .ret();
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, MethodDefinition getIntermediateType, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        CompilerContext context = new CompilerContext();
        definition.declareMethod(
                context,
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                arg("out", BlockBuilder.class))
                .getBody()
                .comment("stateSerializer.serialize(state, out)")
                .pushThis()
                .getField(stateSerializerField)
                .pushThis()
                .getField(stateField)
                .getVariable("out")
                .invokeInterface(AccumulatorStateSerializer.class, "serialize", void.class, Object.class, BlockBuilder.class)
                .ret();
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            FieldDefinition confidenceField,
            FieldDefinition stateSerializerField,
            FieldDefinition stateField,
            @Nullable Method outputFunction,
            boolean approximate)
    {
        Block body = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                arg("groupId", int.class),
                arg("out", BlockBuilder.class))
                .getBody()
                .comment("state.setGroupId(groupId)")
                .pushThis()
                .getField(stateField)
                .getVariable("groupId")
                .intToLong()
                .invokeVirtual(stateField.getType(), "setGroupId", type(void.class), type(long.class));

        if (outputFunction != null) {
            body.comment("output(state, out)")
                    .pushThis()
                    .getField(stateField);
            if (approximate) {
                checkNotNull(confidenceField, "confidenceField is null");
                body.pushThis().getField(confidenceField);
            }
            body.getVariable("out")
                    .invokeStatic(outputFunction);
        }
        else {
            checkArgument(!approximate, "Approximate aggregations must specify an output function");
            body.comment("stateSerializer.serialize(state, out)")
                    .pushThis()
                    .getField(stateSerializerField)
                    .pushThis()
                    .getField(stateField)
                    .getVariable("out")
                    .invokeInterface(AccumulatorStateSerializer.class, "serialize", void.class, Object.class, BlockBuilder.class);
        }
        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            MethodDefinition getFinalType,
            FieldDefinition confidenceField,
            FieldDefinition stateSerializerField,
            FieldDefinition stateField,
            @Nullable
            Method outputFunction,
            boolean approximate)
    {
        Block body = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                arg("out", BlockBuilder.class))
                .getBody();

        if (outputFunction != null) {
            body.comment("output(state, out)")
                    .pushThis()
                    .getField(stateField);
            if (approximate) {
                checkNotNull(confidenceField, "confidenceField is null");
                body.pushThis().getField(confidenceField);
            }
            body.getVariable("out")
                    .invokeStatic(outputFunction);
        }
        else {
            checkArgument(!approximate, "Approximate aggregations must specify an output function");
            body.comment("stateSerializer.serialize(state, out)")
                    .pushThis()
                    .getField(stateSerializerField)
                    .pushThis()
                    .getField(stateField)
                    .getVariable("out")
                    .invokeInterface(AccumulatorStateSerializer.class, "serialize", void.class, Object.class, BlockBuilder.class);
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
        Block body = definition.declareConstructor(
                a(PUBLIC),
                arg("stateSerializer", AccumulatorStateSerializer.class),
                arg("stateFactory", AccumulatorStateFactory.class),
                arg("inputChannels", type(List.class, Integer.class)),
                arg("maskChannel", type(Optional.class, Integer.class)),
                arg("sampleWeightChannel", type(Optional.class, Integer.class)),
                arg("confidence", double.class))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class);

        generateCastCheckNotNullAndAssign(body, stateSerializerField, "stateSerializer");
        generateCastCheckNotNullAndAssign(body, stateFactoryField, "stateFactory");
        generateCastCheckNotNullAndAssign(body, inputChannelsField, "inputChannels");
        generateCastCheckNotNullAndAssign(body, maskChannelField, "maskChannel");
        if (sampleWeightChannelField != null) {
            generateCastCheckNotNullAndAssign(body, sampleWeightChannelField, "sampleWeightChannel");
        }

        String createState;
        if (grouped) {
            createState = "createGroupedState";
        }
        else {
            createState = "createSingleState";
        }

        if (confidenceField != null) {
            body.comment("this.confidence = confidence")
                    .pushThis()
                    .getVariable("confidence")
                    .putField(confidenceField);
        }

        body.comment("this.state = stateFactory.%s()", createState)
                .pushThis()
                .getVariable("stateFactory")
                .invokeInterface(AccumulatorStateFactory.class, createState, Object.class)
                .checkCast(stateField.getType())
                .putField(stateField)
                .ret();
    }

    private static void generateCastCheckNotNullAndAssign(Block block, FieldDefinition field, String variableName)
    {
        block.comment("this.%s = checkNotNull(%s, \"%s is null\"", field.getName(), variableName, variableName)
                .pushThis()
                .getVariable(variableName)
                .checkCast(field.getType())
                .push(variableName + " is null")
                .invokeStatic(Preconditions.class, "checkNotNull", Object.class, Object.class, Object.class)
                .checkCast(field.getType())
                .putField(field);
    }
}
