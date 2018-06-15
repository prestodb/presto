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
package com.facebook.presto.sql.gen;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.JoinHash;
import com.facebook.presto.operator.JoinHashSupplier;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.PagesHash;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.OpCode;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.gen.InputReferenceCompiler.generateInputReference;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static java.util.Objects.requireNonNull;

public class JoinCompiler
{
    private final FunctionRegistry registry;
    private final boolean groupByUsesEqualTo;

    private final LoadingCache<CacheKey, LookupSourceSupplierFactory> lookupSourceFactories = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(1000)
            .build(CacheLoader.from(key ->
                    internalCompileLookupSourceFactory(key.getTypes(), key.getOutputChannels(), key.getJoinChannels(), key.getSortChannel())));

    private final LoadingCache<CacheKey, Class<? extends PagesHashStrategy>> hashStrategies = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(1000)
            .build(CacheLoader.from(key ->
                    internalCompileHashStrategy(key.getTypes(), key.getOutputChannels(), key.getJoinChannels(), key.getSortChannel())));

    public LookupSourceSupplierFactory compileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels, Optional<Integer> sortChannel)
    {
        return compileLookupSourceFactory(types, joinChannels, sortChannel, Optional.empty());
    }

    @Inject
    public JoinCompiler(Metadata metadata, FeaturesConfig config)
    {
        this.registry = requireNonNull(metadata, "metadata is null").getFunctionRegistry();
        this.groupByUsesEqualTo = requireNonNull(config, "config is null").isGroupByUsesEqualTo();
    }

    @Managed
    @Nested
    public CacheStatsMBean getLookupSourceStats()
    {
        return new CacheStatsMBean(lookupSourceFactories);
    }

    @Managed
    @Nested
    public CacheStatsMBean getHashStrategiesStats()
    {
        return new CacheStatsMBean(hashStrategies);
    }

    public LookupSourceSupplierFactory compileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels, Optional<Integer> sortChannel, Optional<List<Integer>> outputChannels)
    {
        return lookupSourceFactories.getUnchecked(new CacheKey(
                types,
                outputChannels.orElse(rangeList(types.size())),
                joinChannels,
                sortChannel));
    }

    public PagesHashStrategyFactory compilePagesHashStrategyFactory(List<Type> types, List<Integer> joinChannels)
    {
        return compilePagesHashStrategyFactory(types, joinChannels, Optional.empty());
    }

    public PagesHashStrategyFactory compilePagesHashStrategyFactory(List<Type> types, List<Integer> joinChannels, Optional<List<Integer>> outputChannels)
    {
        requireNonNull(types, "types is null");
        requireNonNull(joinChannels, "joinChannels is null");
        requireNonNull(outputChannels, "outputChannels is null");

        return new PagesHashStrategyFactory(hashStrategies.getUnchecked(new CacheKey(
                types,
                outputChannels.orElse(rangeList(types.size())),
                joinChannels,
                Optional.empty())));
    }

    private List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private LookupSourceSupplierFactory internalCompileLookupSourceFactory(List<Type> types, List<Integer> outputChannels, List<Integer> joinChannels, Optional<Integer> sortChannel)
    {
        Class<? extends PagesHashStrategy> pagesHashStrategyClass = internalCompileHashStrategy(types, outputChannels, joinChannels, sortChannel);

        Class<? extends LookupSourceSupplier> joinHashSupplierClass = IsolatedClass.isolateClass(
                new DynamicClassLoader(getClass().getClassLoader()),
                LookupSourceSupplier.class,
                JoinHashSupplier.class,
                JoinHash.class,
                PagesHash.class);

        return new LookupSourceSupplierFactory(joinHashSupplierClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    private static FieldDefinition generateInstanceSize(ClassDefinition definition)
    {
        // Store instance size in static field
        FieldDefinition instanceSize = definition.declareField(a(PRIVATE, STATIC, FINAL), "INSTANCE_SIZE", long.class);
        definition.getClassInitializer()
                .getBody()
                .comment("INSTANCE_SIZE = ClassLayout.parseClass(%s.class).instanceSize()", definition.getName())
                .push(definition.getType())
                .invokeStatic(ClassLayout.class, "parseClass", ClassLayout.class, Class.class)
                .invokeVirtual(ClassLayout.class, "instanceSize", int.class)
                .intToLong()
                .putStaticField(instanceSize);
        return instanceSize;
    }

    private Class<? extends PagesHashStrategy> internalCompileHashStrategy(List<Type> types, List<Integer> outputChannels, List<Integer> joinChannels, Optional<Integer> sortChannel)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesHashStrategy"),
                type(Object.class),
                type(PagesHashStrategy.class));

        FieldDefinition instanceSizeField = generateInstanceSize(classDefinition);
        FieldDefinition sizeField = classDefinition.declareField(a(PRIVATE, FINAL), "size", type(long.class));
        List<FieldDefinition> channelFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "channel_" + i, type(List.class, Block.class));
            channelFields.add(channelField);
        }
        List<Type> joinChannelTypes = new ArrayList<>();
        List<FieldDefinition> joinChannelFields = new ArrayList<>();
        for (int i = 0; i < joinChannels.size(); i++) {
            joinChannelTypes.add(types.get(joinChannels.get(i)));
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "joinChannel_" + i, type(List.class, Block.class));
            joinChannelFields.add(channelField);
        }
        FieldDefinition hashChannelField = classDefinition.declareField(a(PRIVATE, FINAL), "hashChannel", type(List.class, Block.class));

        generateConstructor(classDefinition, joinChannels, sizeField, instanceSizeField, channelFields, joinChannelFields, hashChannelField);
        generateGetChannelCountMethod(classDefinition, outputChannels.size());
        generateGetSizeInBytesMethod(classDefinition, sizeField);
        generateAppendToMethod(classDefinition, callSiteBinder, types, outputChannels, channelFields);
        generateHashPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, hashChannelField);
        generateHashRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generateRowEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, true);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, false);
        generatePositionNotDistinctFromRowWithPageMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsRowWithPageMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, true);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, false);
        generateIsPositionNull(classDefinition, joinChannelFields);
        generateCompareSortChannelPositionsMethod(classDefinition, callSiteBinder, types, channelFields, sortChannel);
        generateIsSortChannelPositionNull(classDefinition, channelFields, sortChannel);

        return defineClass(classDefinition, PagesHashStrategy.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            FieldDefinition sizeField,
            FieldDefinition instanceSizeField,
            List<FieldDefinition> channelFields,
            List<FieldDefinition> joinChannelFields,
            FieldDefinition hashChannelField)
    {
        Parameter channels = arg("channels", type(List.class, type(List.class, Block.class)));
        Parameter hashChannel = arg("hashChannel", type(OptionalInt.class));
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), channels, hashChannel);

        Variable thisVariable = constructorDefinition.getThis();
        Variable blockIndex = constructorDefinition.getScope().declareVariable(int.class, "blockIndex");

        BytecodeBlock constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        constructor.comment("this.size = INSTANCE_SIZE")
                .append(thisVariable.setField(sizeField, getStatic(instanceSizeField)));

        constructor.comment("Set channel fields");

        for (int index = 0; index < channelFields.size(); index++) {
            BytecodeExpression channel = channels.invoke("get", Object.class, constantInt(index))
                    .cast(type(List.class, Block.class));

            constructor.append(thisVariable.setField(channelFields.get(index), channel));

            BytecodeBlock loopBody = new BytecodeBlock();

            constructor.comment("for(blockIndex = 0; blockIndex < channel.size(); blockIndex++) { size += channel.get(i).getRetainedSizeInBytes() }")
                    .append(new ForLoop()
                            .initialize(blockIndex.set(constantInt(0)))
                            .condition(new BytecodeBlock()
                                    .append(blockIndex)
                                    .append(channel.invoke("size", int.class))
                                    .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                            .update(new BytecodeBlock().incrementVariable(blockIndex, (byte) 1))
                            .body(loopBody));

            loopBody.append(thisVariable)
                    .append(thisVariable)
                    .getField(sizeField)
                    .append(
                            channel.invoke("get", Object.class, blockIndex)
                                    .cast(type(Block.class))
                                    .invoke("getRetainedSizeInBytes", long.class))
                    .longAdd()
                    .putField(sizeField);
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            BytecodeExpression joinChannel = channels.invoke("get", Object.class, constantInt(joinChannels.get(index)))
                    .cast(type(List.class, Block.class));

            constructor.append(thisVariable.setField(joinChannelFields.get(index), joinChannel));
        }

        constructor.comment("Set hashChannel");
        constructor.append(new IfStatement()
                .condition(hashChannel.invoke("isPresent", boolean.class))
                .ifTrue(thisVariable.setField(
                        hashChannelField,
                        channels.invoke("get", Object.class, hashChannel.invoke("getAsInt", int.class))))
                .ifFalse(thisVariable.setField(
                        hashChannelField,
                        constantNull(hashChannelField.getType()))));
        constructor.ret();
    }

    private static void generateGetChannelCountMethod(ClassDefinition classDefinition, int outputChannelCount)
    {
        classDefinition.declareMethod(
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(outputChannelCount)
                .retInt();
    }

    private static void generateGetSizeInBytesMethod(ClassDefinition classDefinition, FieldDefinition sizeField)
    {
        MethodDefinition getSizeInBytesMethod = classDefinition.declareMethod(a(PUBLIC), "getSizeInBytes", type(long.class));

        Variable thisVariable = getSizeInBytesMethod.getThis();
        getSizeInBytesMethod.getBody()
                .append(thisVariable.getField(sizeField))
                .retLong();
    }

    private static void generateAppendToMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<Integer> outputChannels, List<FieldDefinition> channelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter outputChannelOffset = arg("outputChannelOffset", int.class);
        MethodDefinition appendToMethod = classDefinition.declareMethod(a(PUBLIC), "appendTo", type(void.class), blockIndex, blockPosition, pageBuilder, outputChannelOffset);

        Variable thisVariable = appendToMethod.getThis();
        BytecodeBlock appendToBody = appendToMethod.getBody();

        int pageBuilderOutputChannel = 0;
        for (int outputChannel : outputChannels) {
            Type type = types.get(outputChannel);
            BytecodeExpression typeExpression = constantType(callSiteBinder, type);

            BytecodeExpression block = thisVariable
                    .getField(channelFields.get(outputChannel))
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            appendToBody
                    .comment("%s.appendTo(channel_%s.get(outputChannel), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + %s));", type.getClass(), outputChannel, pageBuilderOutputChannel)
                    .append(typeExpression)
                    .append(block)
                    .append(blockPosition)
                    .append(pageBuilder)
                    .append(outputChannelOffset)
                    .push(pageBuilderOutputChannel++)
                    .append(OpCode.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(Type.class, "appendTo", void.class, Block.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private static void generateIsPositionNull(ClassDefinition classDefinition, List<FieldDefinition> joinChannelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition isPositionNullMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "isPositionNull",
                type(boolean.class),
                blockIndex,
                blockPosition);

        for (FieldDefinition joinChannelField : joinChannelFields) {
            BytecodeExpression block = isPositionNullMethod
                    .getThis()
                    .getField(joinChannelField)
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            IfStatement ifStatement = new IfStatement();
            ifStatement.condition(block.invoke(
                    "isNull",
                    boolean.class,
                    blockPosition));
            ifStatement.ifTrue(constantTrue().ret());
            isPositionNullMethod.getBody().append(ifStatement);
        }

        isPositionNullMethod
                .getBody()
                .append(constantFalse().ret());
    }

    private static void generateHashPositionMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields, FieldDefinition hashChannelField)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "hashPosition",
                type(long.class),
                blockIndex,
                blockPosition);

        Variable thisVariable = hashPositionMethod.getThis();
        BytecodeExpression hashChannel = thisVariable.getField(hashChannelField);
        BytecodeExpression bigintType = constantType(callSiteBinder, BigintType.BIGINT);

        IfStatement ifStatement = new IfStatement();
        ifStatement.condition(notEqual(hashChannel, constantNull(hashChannelField.getType())));
        ifStatement.ifTrue(
                bigintType.invoke(
                        "getLong",
                        long.class,
                        hashChannel.invoke("get", Object.class, blockIndex).cast(Block.class),
                        blockPosition)
                        .ret());

        hashPositionMethod
                .getBody()
                .append(ifStatement);

        Variable resultVariable = hashPositionMethod.getScope().declareVariable(long.class, "result");
        hashPositionMethod.getBody().push(0L).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression block = hashPositionMethod
                    .getThis()
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31L)
                    .append(OpCode.LMUL)
                    .append(typeHashCode(type, block, blockPosition))
                    .append(OpCode.LADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retLong();
    }

    private static void generateHashRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes)
    {
        Parameter position = arg("position", int.class);
        Parameter page = arg("blocks", Page.class);
        MethodDefinition hashRowMethod = classDefinition.declareMethod(a(PUBLIC), "hashRow", type(long.class), position, page);

        Variable resultVariable = hashRowMethod.getScope().declareVariable(long.class, "result");
        hashRowMethod.getBody().push(0L).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression block = page.invoke("getBlock", Block.class, constantInt(index));

            hashRowMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31L)
                    .append(OpCode.LMUL)
                    .append(typeHashCode(type, block, position))
                    .append(OpCode.LADD)
                    .putVariable(resultVariable);
        }

        hashRowMethod
                .getBody()
                .getVariable(resultVariable)
                .retLong();
    }

    private static BytecodeNode typeHashCode(BytecodeExpression type, BytecodeExpression blockRef, BytecodeExpression blockPosition)
    {
        return new IfStatement()
                .condition(blockRef.invoke("isNull", boolean.class, blockPosition))
                .ifTrue(constantLong(0L))
                .ifFalse(type.invoke("hash", long.class, blockRef, blockPosition));
    }

    private static void generateRowEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes)
    {
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftPage = arg("leftPage", Page.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);
        MethodDefinition rowEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "rowEqualsRow",
                type(boolean.class),
                leftPosition,
                leftPage,
                rightPosition,
                rightPage);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = leftPage.invoke("getBlock", Block.class, constantInt(index));

            BytecodeExpression rightBlock = rightPage.invoke("getBlock", Block.class, constantInt(index));

            LabelNode checkNextField = new LabelNode("checkNextField");
            rowEqualsRowMethod
                    .getBody()
                    .append(typeEquals(
                            type,
                            leftBlock,
                            leftPosition,
                            rightBlock,
                            rightPosition))
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        rowEqualsRowMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static void generatePositionEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields,
            boolean ignoreNulls)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);
        MethodDefinition positionEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                ignoreNulls ? "positionEqualsRowIgnoreNulls" : "positionEqualsRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                rightPage);

        Variable thisVariable = positionEqualsRowMethod.getThis();

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = rightPage.invoke("getBlock", Block.class, constantInt(index));
            BytecodeNode equalityCondition;
            if (ignoreNulls) {
                equalityCondition = typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightPosition);
            }
            else {
                equalityCondition = typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightPosition);
            }

            LabelNode checkNextField = new LabelNode("checkNextField");
            positionEqualsRowMethod
                    .getBody()
                    .append(equalityCondition)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        positionEqualsRowMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static void generatePositionEqualsRowWithPageMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter page = arg("page", Page.class);
        Parameter rightChannels = arg("rightChannels", int[].class);

        MethodDefinition positionEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "positionEqualsRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                page,
                rightChannels);

        Variable thisVariable = positionEqualsRowMethod.getThis();
        BytecodeBlock body = positionEqualsRowMethod.getBody();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));
            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = page.invoke("getBlock", Block.class, rightChannels.getElement(index));
            body.append(new IfStatement()
                    .condition(typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightPosition))
                    .ifFalse(constantFalse().ret()));
        }
        body.append(constantTrue().ret());
    }

    private void generatePositionNotDistinctFromRowWithPageMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter page = arg("page", Page.class);
        Parameter rightChannels = arg("rightChannels", int[].class);

        MethodDefinition positionNotDistinctFromRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "positionNotDistinctFromRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                page,
                rightChannels);

        Variable thisVariable = positionNotDistinctFromRowMethod.getThis();
        Scope scope = positionNotDistinctFromRowMethod.getScope();
        BytecodeBlock body = positionNotDistinctFromRowMethod.getBody();
        if (groupByUsesEqualTo) {
            // positionNotDistinctFromRow delegates to positionEqualsRow when groupByUsesEqualTo is set.
            body.append(thisVariable.invoke(
                    "positionEqualsRow",
                    boolean.class,
                    leftBlockIndex,
                    leftBlockPosition,
                    rightPosition,
                    page,
                    rightChannels).ret());
            return;
        }
        scope.declareVariable("wasNull", body, constantFalse());
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);
            BytecodeExpression rightBlock = page.invoke("getBlock", Block.class, rightChannels.getElement(index));
            Type type = joinChannelTypes.get(index);
            // This is a hack for performance reasons.
            // Type.equalTo takes two pairs of Block+position.
            // On the other hand, NOT_DISTINCT_FROM is an operator. It takes two Slices.
            // As a result, two Slices must be constructed for each invocation, which has a nontrivial cost.
            // For these types, their equal semantics is known to be the same as not-distinct-from except for null values.
            //
            // The plan is to allow scalar function to optionally provide an additional implementation using Block+position calling convention.
            // At that point, we'll be able to fully deprecate Type.equalTo (and friends) and remove this hack.
            if (type.getJavaType().equals(Slice.class)) {
                switch(type.getTypeSignature().getBase()) {
                    case StandardTypes.CHAR:
                    case StandardTypes.IPADDRESS:
                    case StandardTypes.JSON:
                    case StandardTypes.DECIMAL:
                    case StandardTypes.VARBINARY:
                    case StandardTypes.VARCHAR:
                        body.append(new IfStatement()
                                .condition(typeEquals(constantType(callSiteBinder, type), leftBlock, leftBlockPosition, rightBlock, rightPosition))
                                .ifFalse(constantFalse().ret()));
                        continue;
                }
            }
            ScalarFunctionImplementation operator = registry.getScalarFunctionImplementation(registry.resolveOperator(OperatorType.IS_DISTINCT_FROM, ImmutableList.of(type, type)));
            Binding binding = callSiteBinder.bind(operator.getMethodHandle());
            List<BytecodeNode> argumentsBytecode = new ArrayList<>();
            argumentsBytecode.add(generateInputReference(callSiteBinder, scope, type, leftBlock, leftBlockPosition));
            argumentsBytecode.add(generateInputReference(callSiteBinder, scope, type, rightBlock, rightPosition));

            body.append(new IfStatement()
                    .condition(BytecodeUtils.generateInvocation(scope, "isDistinctFrom", operator, Optional.empty(), argumentsBytecode, callSiteBinder))
                    .ifTrue(constantFalse().ret()));
        }
        body.append(constantTrue().ret());
    }

    private static void generatePositionEqualsPositionMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields,
            boolean ignoreNulls)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightBlockIndex = arg("rightBlockIndex", int.class);
        Parameter rightBlockPosition = arg("rightBlockPosition", int.class);
        MethodDefinition positionEqualsPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                ignoreNulls ? "positionEqualsPositionIgnoreNulls" : "positionEqualsPosition",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightBlockIndex,
                rightBlockPosition);

        Variable thisVariable = positionEqualsPositionMethod.getThis();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, rightBlockIndex)
                    .cast(Block.class);

            BytecodeNode equalityCondition;
            if (ignoreNulls) {
                equalityCondition = typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            }
            else {
                equalityCondition = typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            }

            LabelNode checkNextField = new LabelNode("checkNextField");
            positionEqualsPositionMethod
                    .getBody()
                    .append(equalityCondition)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        positionEqualsPositionMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static void generateCompareSortChannelPositionsMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> types,
            List<FieldDefinition> channelFields,
            Optional<Integer> sortChannel)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightBlockIndex = arg("rightBlockIndex", int.class);
        Parameter rightBlockPosition = arg("rightBlockPosition", int.class);
        MethodDefinition compareMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "compareSortChannelPositions",
                type(int.class),
                leftBlockIndex,
                leftBlockPosition,
                rightBlockIndex,
                rightBlockPosition);

        if (!sortChannel.isPresent()) {
            compareMethod.getBody()
                    .append(newInstance(UnsupportedOperationException.class))
                    .throwObject();
            return;
        }

        Variable thisVariable = compareMethod.getThis();

        int index = sortChannel.get();
        BytecodeExpression type = constantType(callSiteBinder, types.get(index));

        BytecodeExpression leftBlock = thisVariable
                .getField(channelFields.get(index))
                .invoke("get", Object.class, leftBlockIndex)
                .cast(Block.class);

        BytecodeExpression rightBlock = thisVariable
                .getField(channelFields.get(index))
                .invoke("get", Object.class, rightBlockIndex)
                .cast(Block.class);

        BytecodeNode comparison = type.invoke("compareTo", int.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition).ret();

        compareMethod
                .getBody()
                .append(comparison);
    }

    private static void generateIsSortChannelPositionNull(
            ClassDefinition classDefinition,
            List<FieldDefinition> channelFields,
            Optional<Integer> sortChannel)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition isSortChannelPositionNullMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "isSortChannelPositionNull",
                type(boolean.class),
                blockIndex,
                blockPosition);

        if (!sortChannel.isPresent()) {
            isSortChannelPositionNullMethod.getBody()
                    .append(newInstance(UnsupportedOperationException.class))
                    .throwObject();
            return;
        }

        Variable thisVariable = isSortChannelPositionNullMethod.getThis();

        int index = sortChannel.get();

        BytecodeExpression block = thisVariable
                .getField(channelFields.get(index))
                .invoke("get", Object.class, blockIndex)
                .cast(Block.class);

        BytecodeNode isNull = block.invoke("isNull", boolean.class, blockPosition).ret();

        isSortChannelPositionNullMethod
                .getBody()
                .append(isNull);
    }

    private static BytecodeNode typeEquals(
            BytecodeExpression type,
            BytecodeExpression leftBlock,
            BytecodeExpression leftBlockPosition,
            BytecodeExpression rightBlock,
            BytecodeExpression rightBlockPosition)
    {
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IOR);

        ifStatement.ifTrue()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IAND);

        ifStatement.ifFalse().append(typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition));

        return ifStatement;
    }

    private static BytecodeNode typeEqualsIgnoreNulls(
            BytecodeExpression type,
            BytecodeExpression leftBlock,
            BytecodeExpression leftBlockPosition,
            BytecodeExpression rightBlock,
            BytecodeExpression rightBlockPosition)
    {
        return type.invoke("equalTo", boolean.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
    }

    public static class LookupSourceSupplierFactory
    {
        private final Constructor<? extends LookupSourceSupplier> constructor;
        private final PagesHashStrategyFactory pagesHashStrategyFactory;

        public LookupSourceSupplierFactory(Class<? extends LookupSourceSupplier> joinHashSupplierClass, PagesHashStrategyFactory pagesHashStrategyFactory)
        {
            this.pagesHashStrategyFactory = pagesHashStrategyFactory;
            try {
                constructor = joinHashSupplierClass.getConstructor(Session.class, PagesHashStrategy.class, LongArrayList.class, List.class, Optional.class, Optional.class, List.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        public LookupSourceSupplier createLookupSourceSupplier(
                Session session,
                LongArrayList addresses,
                List<List<Block>> channels,
                OptionalInt hashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel);
            try {
                return constructor.newInstance(session, pagesHashStrategy, addresses, channels, filterFunctionFactory, sortChannel, searchFunctionFactories);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class PagesHashStrategyFactory
    {
        private final Constructor<? extends PagesHashStrategy> constructor;

        public PagesHashStrategyFactory(Class<? extends PagesHashStrategy> pagesHashStrategyClass)
        {
            try {
                constructor = pagesHashStrategyClass.getConstructor(List.class, OptionalInt.class);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        public PagesHashStrategy createPagesHashStrategy(List<? extends List<Block>> channels, OptionalInt hashChannel)
        {
            try {
                return constructor.newInstance(channels, hashChannel);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class CacheKey
    {
        private final List<Type> types;
        private final List<Integer> outputChannels;
        private final List<Integer> joinChannels;
        private final Optional<Integer> sortChannel;

        private CacheKey(List<? extends Type> types, List<Integer> outputChannels, List<Integer> joinChannels, Optional<Integer> sortChannel)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.joinChannels = ImmutableList.copyOf(requireNonNull(joinChannels, "joinChannels is null"));
            this.sortChannel = requireNonNull(sortChannel, "sortChannel is null");
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private List<Integer> getOutputChannels()
        {
            return outputChannels;
        }

        private List<Integer> getJoinChannels()
        {
            return joinChannels;
        }

        private Optional<Integer> getSortChannel()
        {
            return sortChannel;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types, outputChannels, joinChannels, sortChannel);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            return Objects.equals(this.types, other.types) &&
                    Objects.equals(this.outputChannels, other.outputChannels) &&
                    Objects.equals(this.joinChannels, other.joinChannels) &&
                    Objects.equals(this.sortChannel, other.sortChannel);
        }
    }
}
