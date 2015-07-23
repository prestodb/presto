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
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
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
import static com.facebook.presto.byteCode.OpCode.NOP;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.not;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.countInputChannels;
import static com.facebook.presto.sql.gen.ByteCodeUtils.invoke;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.sql.gen.SqlTypeByteCodeExpression.constantType;
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
            generateGroupedEvaluateFinal(definition, confidenceField, stateField, metadata.getOutputFunction(), metadata.isApproximate(), callSiteBinder);
        }
        else {
            generateEvaluateFinal(definition, confidenceField, stateField, metadata.getOutputFunction(), metadata.isApproximate(), callSiteBinder);
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
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        Parameter page = arg("page", Page.class);
        parameters.add(page);

        MethodDefinition method = definition.declareMethod(a(PUBLIC), "addInput", type(void.class), parameters.build());
        Scope scope = method.getScope();
        Block body = method.getBody();
        Variable thisVariable = method.getThis();

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        List<Variable> parameterVariables = new ArrayList<>();
        for (int i = 0; i < countInputChannels(parameterMetadatas); i++) {
            parameterVariables.add(scope.declareVariable(com.facebook.presto.spi.block.Block.class, "block" + i));
        }
        Variable masksBlock = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "masksBlock");
        Variable sampleWeightsBlock = null;
        if (sampleWeightChannelField != null) {
            sampleWeightsBlock = scope.declareVariable(com.facebook.presto.spi.block.Block.class, "sampleWeightsBlock");
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
        Block block = generateInputForLoop(stateField, parameterMetadatas, inputFunction, scope, parameterVariables, masksBlock, sampleWeightsBlock, callSiteBinder, grouped);

        body.append(block);
        body.ret();
    }

    private static Block generateInputForLoop(
            FieldDefinition stateField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            Scope scope,
            List<Variable> parameterVariables,
            Variable masksBlock,
            @Nullable Variable sampleWeightsBlock,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        // For-loop over rows
        Variable page = scope.getVariable("page");
        Variable positionVariable = scope.declareVariable(int.class, "position");
        Variable sampleWeightVariable = null;
        if (sampleWeightsBlock != null) {
            sampleWeightVariable = scope.declareVariable(long.class, "sampleWeight");
        }
        Variable rowsVariable = scope.declareVariable(int.class, "rows");

        Block block = new Block()
                .append(page)
                .invokeVirtual(Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable)
                .initializeVariable(positionVariable);
        if (sampleWeightVariable != null) {
            block.initializeVariable(sampleWeightVariable);
        }

        ByteCodeNode loopBody = generateInvokeInputFunction(scope, stateField, positionVariable, sampleWeightVariable, parameterVariables, parameterMetadatas, inputFunction, callSiteBinder, grouped);

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
                        .condition(new Block()
                                .getVariable(variableDefinition)
                                .getVariable(positionVariable)
                                .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                        .ifFalse(loopBody);
            }
        }

        // Check that sample weight is > 0 (also checks the mask)
        if (sampleWeightVariable != null) {
            loopBody = generateComputeSampleWeightAndCheckGreaterThanZero(loopBody, sampleWeightVariable, masksBlock, sampleWeightsBlock, positionVariable);
        }
        // Otherwise just check the mask
        else {
            loopBody = new IfStatement("if(testMask(%s, position))", masksBlock.getName())
                    .condition(new Block()
                            .getVariable(masksBlock)
                            .getVariable(positionVariable)
                            .invokeStatic(CompilerOperations.class, "testMask", boolean.class, com.facebook.presto.spi.block.Block.class, int.class))
                    .ifTrue(loopBody);
        }

        block.append(new ForLoop()
                .initialize(new Block().putVariable(positionVariable, 0))
                .condition(new Block()
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block().incrementVariable(positionVariable, (byte) 1))
                .body(loopBody));

        return block;
    }

    private static ByteCodeNode generateComputeSampleWeightAndCheckGreaterThanZero(ByteCodeNode body, Variable sampleWeight, Variable masks, Variable sampleWeights, Variable position)
    {
        Block block = new Block()
                .comment("sampleWeight = computeSampleWeight(masks, sampleWeights, position);")
                .getVariable(masks)
                .getVariable(sampleWeights)
                .getVariable(position)
                .invokeStatic(ApproximateUtils.class, "computeSampleWeight", long.class, com.facebook.presto.spi.block.Block.class, com.facebook.presto.spi.block.Block.class, int.class)
                .putVariable(sampleWeight);

        block.append(new IfStatement("if(sampleWeight > 0)")
                .condition(new Block()
                        .getVariable(sampleWeight)
                        .invokeStatic(CompilerOperations.class, "longGreaterThanZero", boolean.class, long.class))
                .ifTrue(body)
                .ifFalse(NOP));

        return block;
    }

    private static Block generateInvokeInputFunction(
            Scope scope,
            FieldDefinition stateField,
            Variable position,
            @Nullable Variable sampleWeight,
            List<Variable> parameterVariables,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle inputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        Block block = new Block();

        if (grouped) {
            generateSetGroupIdFromGroupIdsBlock(scope, stateField, block);
        }

        block.comment("Call input function with unpacked Block arguments");

        Class<?>[] parameters = inputFunction.type().parameterArray();
        int inputChannel = 0;
        for (int i = 0; i < parameters.length; i++) {
            ParameterMetadata parameterMetadata = parameterMetadatas.get(i);
            switch (parameterMetadata.getParameterType()) {
                case STATE:
                    block.append(scope.getThis().getField(stateField));
                    break;
                case BLOCK_INDEX:
                    block.getVariable(position);
                    break;
                case SAMPLE_WEIGHT:
                    checkNotNull(sampleWeight, "sampleWeight is null");
                    block.getVariable(sampleWeight);
                    break;
                case BLOCK_INPUT_CHANNEL:
                case NULLABLE_BLOCK_INPUT_CHANNEL:
                    block.getVariable(parameterVariables.get(inputChannel));
                    inputChannel++;
                    break;
                case INPUT_CHANNEL:
                    Block getBlockByteCode = new Block()
                            .getVariable(parameterVariables.get(inputChannel));
                    pushStackType(scope, block, parameterMetadata.getSqlType(), getBlockByteCode, parameters[i], callSiteBinder);
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
    private static void pushStackType(Scope scope, Block block, Type sqlType, Block getBlockByteCode, Class<?> parameter, CallSiteBinder callSiteBinder)
    {
        Variable position = scope.getVariable("position");
        if (parameter == long.class) {
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
            block.comment("%s.getSlice(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getSlice", Slice.class, com.facebook.presto.spi.block.Block.class, int.class);
        }
        else {
            block.comment("%s.getObject(block, position)", sqlType.getTypeSignature())
                    .append(constantType(callSiteBinder, sqlType))
                    .append(getBlockByteCode)
                    .append(position)
                    .invokeInterface(Type.class, "getObject", Object.class, com.facebook.presto.spi.block.Block.class, int.class);
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
        MethodDefinition method = declareAddIntermediate(definition, grouped);
        Scope scope = method.getScope();
        Block body = method.getBody();
        Variable thisVariable = method.getThis();

        Variable block = scope.getVariable("block");
        Variable scratchState = scope.declareVariable(singleStateClass, "scratchState");
        Variable position = scope.declareVariable(int.class, "position");

        body.comment("scratchState = stateFactory.createSingleState();")
                .append(thisVariable.getField(stateFactoryField))
                .invokeInterface(AccumulatorStateFactory.class, "createSingleState", Object.class)
                .checkCast(scratchState.getType())
                .putVariable(scratchState);

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        Block loopBody = new Block();

        if (grouped) {
            Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
            loopBody.append(thisVariable.getField(stateField).invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
        }

        loopBody.append(thisVariable.getField(stateSerializerField).invoke("deserialize", void.class, block, position, scratchState.cast(Object.class)));

        loopBody.comment("combine(state, scratchState)")
                .append(thisVariable.getField(stateField))
                .append(scratchState)
                .append(invoke(callSiteBinder.bind(combineFunction), "combine"));

        if (grouped) {
            // skip rows with null group id
            IfStatement ifStatement = new IfStatement("if (!groupIdsBlock.isNull(position))")
                    .condition(not(scope.getVariable("groupIdsBlock").invoke("isNull", boolean.class, position)))
                    .ifTrue(loopBody);

            loopBody = new Block().append(ifStatement);
        }

        body.append(generateBlockNonNullPositionForLoop(scope, position, loopBody))
                .ret();
    }

    private static void generateSetGroupIdFromGroupIdsBlock(Scope scope, FieldDefinition stateField, Block block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        Variable position = scope.getVariable("position");
        ByteCodeExpression state = scope.getThis().getField(stateField);
        block.append(state.invoke("setGroupId", void.class, groupIdsBlock.invoke("getGroupId", long.class, position)));
    }

    private static void generateEnsureCapacity(Scope scope, FieldDefinition stateField, Block block)
    {
        Variable groupIdsBlock = scope.getVariable("groupIdsBlock");
        ByteCodeExpression state = scope.getThis().getField(stateField);
        block.append(state.invoke("ensureCapacity", void.class, groupIdsBlock.invoke("getGroupCount", long.class)));
    }

    private static MethodDefinition declareAddIntermediate(ClassDefinition definition, boolean grouped)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        if (grouped) {
            parameters.add(arg("groupIdsBlock", GroupByIdBlock.class));
        }
        parameters.add(arg("block", com.facebook.presto.spi.block.Block.class));

        return definition.declareMethod(
                a(PUBLIC),
                "addIntermediate",
                type(void.class),
                parameters.build());
    }

    private static void generateAddIntermediateAsIntermediateInput(
            ClassDefinition definition,
            FieldDefinition stateField,
            List<ParameterMetadata> parameterMetadatas,
            MethodHandle intermediateInputFunction,
            CallSiteBinder callSiteBinder,
            boolean grouped)
    {
        MethodDefinition method = declareAddIntermediate(definition, grouped);
        Scope scope = method.getScope();
        Block body = method.getBody();

        if (grouped) {
            generateEnsureCapacity(scope, stateField, body);
        }

        Variable positionVariable = scope.declareVariable(int.class, "position");

        Block loopBody = generateInvokeInputFunction(scope, stateField, positionVariable, null, ImmutableList.of(scope.getVariable("block")), parameterMetadatas, intermediateInputFunction, callSiteBinder, grouped);

        if (grouped) {
            // skip rows with null group id
            IfStatement ifStatement = new IfStatement("if (!groupIdsBlock.isNull(position))")
                    .condition(not(scope.getVariable("groupIdsBlock").invoke("isNull", boolean.class, positionVariable)))
                    .ifTrue(loopBody);

            loopBody = new Block().append(ifStatement);
        }

        body.append(generateBlockNonNullPositionForLoop(scope, positionVariable, loopBody))
                .ret();
    }

    // Generates a for-loop with a local variable named "position" defined, with the current position in the block,
    // loopBody will only be executed for non-null positions in the Block
    private static Block generateBlockNonNullPositionForLoop(Scope scope, Variable positionVariable, Block loopBody)
    {
        Variable rowsVariable = scope.declareVariable(int.class, "rows");
        Variable blockVariable = scope.getVariable("block");

        Block block = new Block()
                .append(blockVariable)
                .invokeInterface(com.facebook.presto.spi.block.Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        IfStatement ifStatement = new IfStatement("if(!block.isNull(position))")
                .condition(new Block()
                        .append(blockVariable)
                        .append(positionVariable)
                        .invokeInterface(com.facebook.presto.spi.block.Block.class, "isNull", boolean.class, int.class))
                .ifFalse(loopBody);

        block.append(new ForLoop()
                .initialize(positionVariable.set(constantInt(0)))
                .condition(new Block()
                        .append(positionVariable)
                        .append(rowsVariable)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block().incrementVariable(positionVariable, (byte) 1))
                .body(ifStatement));

        return block;
    }

    private static void generateGroupedEvaluateIntermediate(ClassDefinition definition, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateIntermediate", type(void.class), groupId, out);

        Variable thisVariable = method.getThis();
        ByteCodeExpression state = thisVariable.getField(stateField);
        ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);

        method.getBody()
                .append(state.invoke("setGroupId", void.class, groupId.cast(long.class)))
                .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                .ret();
    }

    private static void generateEvaluateIntermediate(ClassDefinition definition, FieldDefinition stateSerializerField, FieldDefinition stateField)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateIntermediate",
                type(void.class),
                out);

        Variable thisVariable = method.getThis();
        ByteCodeExpression stateSerializer = thisVariable.getField(stateSerializerField);
        ByteCodeExpression state = thisVariable.getField(stateField);

        method.getBody()
                .append(stateSerializer.invoke("serialize", void.class, state.cast(Object.class), out))
                .ret();
    }

    private static void generateGroupedEvaluateFinal(
            ClassDefinition definition,
            FieldDefinition confidenceField,
            FieldDefinition stateField,
            MethodHandle outputFunction,
            boolean approximate,
            CallSiteBinder callSiteBinder)
    {
        Parameter groupId = arg("groupId", int.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(a(PUBLIC), "evaluateFinal", type(void.class), groupId, out);

        Block body = method.getBody();
        Variable thisVariable = method.getThis();

        ByteCodeExpression state = thisVariable.getField(stateField);

        body.append(state.invoke("setGroupId", void.class, groupId.cast(long.class)));

        body.comment("output(state, out)");
        body.append(state);
        if (approximate) {
            checkNotNull(confidenceField, "confidenceField is null");
            body.append(thisVariable.getField(confidenceField));
        }
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

        body.ret();
    }

    private static void generateEvaluateFinal(
            ClassDefinition definition,
            FieldDefinition confidenceField,
            FieldDefinition stateField,
            MethodHandle outputFunction,
            boolean approximate,
            CallSiteBinder callSiteBinder)
    {
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC),
                "evaluateFinal",
                type(void.class),
                out);

        Block body = method.getBody();
        Variable thisVariable = method.getThis();

        ByteCodeExpression state = thisVariable.getField(stateField);

        body.comment("output(state, out)");
        body.append(state);
        if (approximate) {
            checkNotNull(confidenceField, "confidenceField is null");
            body.append(thisVariable.getField(confidenceField));
        }
        body.append(out);
        body.append(invoke(callSiteBinder.bind(outputFunction), "output"));

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
        Parameter stateSerializer = arg("stateSerializer", AccumulatorStateSerializer.class);
        Parameter stateFactory = arg("stateFactory", AccumulatorStateFactory.class);
        Parameter inputChannels = arg("inputChannels", type(List.class, Integer.class));
        Parameter maskChannel = arg("maskChannel", type(Optional.class, Integer.class));
        Parameter sampleWeightChannel = arg("sampleWeightChannel", type(Optional.class, Integer.class));
        Parameter confidence = arg("confidence", double.class);
        MethodDefinition method = definition.declareConstructor(
                a(PUBLIC),
                stateSerializer,
                stateFactory,
                inputChannels,
                maskChannel,
                sampleWeightChannel,
                confidence);

        Block body = method.getBody();
        Variable thisVariable = method.getThis();

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
