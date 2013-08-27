/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.gen;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.control.WhileLoop;
import com.facebook.presto.byteCode.control.WhileLoop.WhileLoopBuilder;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.AbstractFilterAndProjectOperator;
import com.facebook.presto.operator.AbstractFilterAndProjectOperator.AbstractFilterAndProjectIterator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.byteCode.control.ForLoop.forLoopBuilder;
import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.nCopies;

public class ExpressionCompiler
{
    private static final Logger log = Logger.get(ExpressionCompiler.class);

    private static final AtomicLong CLASS_ID = new AtomicLong();

    private static final boolean DUMP_BYTE_CODE_TREE = false;
    private static final boolean DUMP_BYTE_CODE_RAW = false;
    private static final boolean RUN_ASM_VERIFIER = false; // verifier doesn't work right now
    private static final AtomicReference<String> DUMP_CLASS_FILES_TO = new AtomicReference<>();

    private final Method bootstrapMethod;
    private final BootstrapFunctionBinder bootstrapFunctionBinder;

    private final LoadingCache<OperatorCacheKey, OperatorFactory> operatorFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<OperatorCacheKey, OperatorFactory>()
            {
                @Override
                public OperatorFactory load(OperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileFilterAndProjectOperator(key.getFilter(), key.getProjections(), key.getInputTypes());
                }
            });

    private final LoadingCache<ExpressionCacheKey, Function<Session, FilterFunction>> filters = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<ExpressionCacheKey, Function<Session, FilterFunction>>()
            {
                @Override
                public Function<Session, FilterFunction> load(ExpressionCacheKey key)
                        throws Exception
                {
                    return internalCompileFilterFunction(key.getExpression(), key.getInputTypes());
                }
            });

    private final LoadingCache<ExpressionCacheKey, Function<Session, ProjectionFunction>> projections = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<ExpressionCacheKey, Function<Session, ProjectionFunction>>()
            {
                @Override
                public Function<Session, ProjectionFunction> load(ExpressionCacheKey key)
                        throws Exception
                {
                    return internalCompileProjectionFunction(key.getExpression(), key.getInputTypes());
                }
            });


    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.bootstrapFunctionBinder = new BootstrapFunctionBinder(checkNotNull(metadata, "metadata is null"));

        // code gen a bootstrap class
        try {
            ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(null),
                    a(PUBLIC, FINAL),
                    typeFromPathName("Bootstrap" + CLASS_ID.incrementAndGet()),
                    type(Object.class));

            FieldDefinition bootstrapField = classDefinition.declareField(a(PUBLIC, STATIC, FINAL), "BOOTSTRAP", type(AtomicReference.class, BootstrapFunctionBinder.class));

            classDefinition.getClassInitializer()
                    .getBody()
                    .newObject(AtomicReference.class)
                    .dup()
                    .invokeConstructor(AtomicReference.class)
                    .putStaticField(bootstrapField);

            classDefinition.declareMethod(new CompilerContext(null),
                    a(PUBLIC, STATIC),
                    "bootstrap",
                    type(CallSite.class),
                    arg("lookup", Lookup.class),
                    arg("name", String.class),
                    arg("type", MethodType.class),
                    arg("bindingId", long.class))
                    .getBody()
                    .comment("return BOOTSTRAP.get().bootstrap(name, type, bindingId);")
                    .getStaticField(bootstrapField)
                    .invokeVirtual(AtomicReference.class, "get", Object.class)
                    .checkCast(BootstrapFunctionBinder.class)
                    .getVariable("name")
                    .getVariable("type")
                    .getVariable("bindingId")
                    .invokeVirtual(BootstrapFunctionBinder.class, "bootstrap", CallSite.class, String.class, MethodType.class, long.class)
                    .retObject();

            Class<?> bootstrapClass = defineClasses(ImmutableList.of(classDefinition), new DynamicClassLoader()).values().iterator().next();

            AtomicReference<BootstrapFunctionBinder> bootstrapReference = (AtomicReference<BootstrapFunctionBinder>) bootstrapClass.getField("BOOTSTRAP").get(null);
            bootstrapReference.set(bootstrapFunctionBinder);

            bootstrapMethod = bootstrapClass.getMethod("bootstrap", Lookup.class, String.class, MethodType.class, long.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public OperatorFactory compileFilterAndProjectOperator(Expression filter, List<Expression> projections, Map<Input, Type> inputTypes)
    {
        return operatorFactories.getUnchecked(new OperatorCacheKey(filter, projections, inputTypes));
    }

    public FilterFunction compileFilterFunction(Expression expression, Map<Input, Type> inputTypes, Session session)
    {
        return filters.getUnchecked(new ExpressionCacheKey(expression, inputTypes)).apply(session);
    }

    public ProjectionFunction compileProjectionFunction(Expression expression, Map<Input, Type> inputTypes, Session session)
    {
        return projections.getUnchecked(new ExpressionCacheKey(expression, inputTypes)).apply(session);
    }

    @VisibleForTesting
    public OperatorFactory internalCompileFilterAndProjectOperator(Expression filter, List<Expression> projections, Map<Input, Type> inputTypes)
    {
        DynamicClassLoader classLoader = createClassLoader();

        // create filter and project page iterator class
        TypedPageIteratorClass typedPageIteratorClass = compileFilterAndProjectIterator(filter, projections, inputTypes, classLoader);

        // create an operator for the class
        Class<? extends Operator> operatorClass = compileOperatorClass(typedPageIteratorClass.getPageIteratorClass(), classLoader);

        // create a factory for the operator
        return compileOperatorFactoryClass(typedPageIteratorClass.getTupleInfos(), operatorClass, classLoader);
    }

    private DynamicClassLoader createClassLoader()
    {
        return new DynamicClassLoader(bootstrapMethod.getDeclaringClass().getClassLoader());
    }

    private TypedPageIteratorClass compileFilterAndProjectIterator(Expression filter,
            List<Expression> projections,
            Map<Input, Type> inputTypes,
            DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProjectIterator_" + CLASS_ID.incrementAndGet()),
                type(AbstractFilterAndProjectIterator.class));

        // declare fields
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);
        FieldDefinition operatorStatsField = classDefinition.declareField(a(PRIVATE, FINAL), "operatorStats", OperatorStats.class);
        FieldDefinition currentCompletedSizeField = classDefinition.declareField(a(PRIVATE), "currentCompletedSize", long.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("tupleInfos", type(Iterable.class, TupleInfo.class)),
                arg("pageIterator", PageIterator.class),
                arg("session", Session.class),
                arg("operatorStats", OperatorStats.class))
                .getBody()
                .comment("super(tupleInfos, pageIterator);")
                .pushThis()
                .getVariable("tupleInfos")
                .getVariable("pageIterator")
                .getVariable("operatorStats")
                .invokeConstructor(AbstractFilterAndProjectIterator.class, Iterable.class, PageIterator.class, OperatorStats.class)
                .comment("this.session = session;")
                .pushThis()
                .getVariable("session")
                .putField(sessionField)
                .comment("this.operatorStats = operatorStats;")
                .pushThis()
                .getVariable("operatorStats")
                .putField(operatorStatsField)
                .ret();

        generateFilterAndProjectCursorMethod(classDefinition, projections, operatorStatsField, currentCompletedSizeField);
        generateFilterAndProjectIteratorMethod(classDefinition, projections, inputTypes);

        //
        // filter method
        //
        generateFilterMethod(classDefinition, filter, inputTypes, true);
        generateFilterMethod(classDefinition, filter, inputTypes, false);

        //
        // project methods
        //
        List<TupleInfo> tupleInfos = new ArrayList<>();
        int projectionIndex = 0;
        for (Expression projection : projections) {
            Class<?> type = generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, inputTypes, true);
            generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, inputTypes, false);
            if (type == boolean.class) {
                tupleInfos.add(TupleInfo.SINGLE_BOOLEAN);
            }
            // todo remove assumption that void is a long
            else if (type == long.class || type == void.class) {
                tupleInfos.add(TupleInfo.SINGLE_LONG);
            }
            else if (type == double.class) {
                tupleInfos.add(TupleInfo.SINGLE_DOUBLE);
            }
            else if (type == Slice.class) {
                tupleInfos.add(TupleInfo.SINGLE_VARBINARY);
            }
            else {
                throw new IllegalStateException("Type " + type.getName() + "can be output");
            }
            projectionIndex++;
        }

        //
        // toString method
        //
        classDefinition.declareMethod(new CompilerContext(bootstrapMethod), a(PUBLIC), "toString", type(String.class))
                .getBody()
                .push(toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", filter)
                        .add("projections", projections)
                        .toString())
                .retObject();

        Class<? extends PageIterator> filterAndProjectClass = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(PageIterator.class);
        return new TypedPageIteratorClass(filterAndProjectClass, tupleInfos);
    }

    private void generateFilterAndProjectIteratorMethod(ClassDefinition classDefinition,
            List<Expression> projections,
            Map<Input, Type> inputTypes)
    {
        MethodDefinition filterAndProjectMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filterAndProjectRowOriented",
                type(void.class),
                arg("blocks", com.facebook.presto.block.Block[].class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = filterAndProjectMethod.getCompilerContext();

        LocalVariableDefinition positionVariable = compilerContext.declareVariable(int.class, "position");

        LocalVariableDefinition rowsVariable = compilerContext.declareVariable(int.class, "rows");
        filterAndProjectMethod.getBody()
                .comment("int rows = blocks[0].getPositionCount();")
                .getVariable("blocks")
                .push(0)
                .getObjectArrayElement()
                .invokeInterface(com.facebook.presto.block.Block.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);


        List<LocalVariableDefinition> cursorVariables = new ArrayList<>();
        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            LocalVariableDefinition cursorVariable = compilerContext.declareVariable(BlockCursor.class, "cursor_" + i);
            cursorVariables.add(cursorVariable);
            filterAndProjectMethod.getBody()
                    .comment("BlockCursor %s = blocks[%s].cursor();", cursorVariable.getName(), i)
                    .getVariable("blocks")
                    .push(i)
                    .getObjectArrayElement()
                    .invokeInterface(com.facebook.presto.block.Block.class, "cursor", BlockCursor.class)
                    .putVariable(cursorVariable);
        }

        //
        // for loop body
        //

        // for (position = 0; position < rows; position++)
        ForLoopBuilder forLoop = forLoopBuilder(compilerContext)
                .comment("for (position = 0; position < rows; position++)")
                .initialize(new Block(compilerContext).putVariable(positionVariable, 0))
                .condition(new Block(compilerContext)
                        .getVariable(positionVariable)
                        .getVariable(rowsVariable)
                        .invokeStatic(Operations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(compilerContext).incrementVariable(positionVariable, (byte) 1));

        Block forLoopBody = new Block(compilerContext);

        // cursor.advanceNextPosition()
        for (LocalVariableDefinition cursorVariable : cursorVariables) {
            forLoopBody
                    .comment("checkState(%s.advanceNextPosition());", cursorVariable.getName())
                    .getVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext)
                .comment("if (filter(cursors...)");
        Block condition = new Block(compilerContext);
        condition.pushThis();
        for (int channel = 0; channel < channels; channel++) {
            condition.getVariable("cursor_" + channel);
        }
        condition.invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), nCopies(channels, type(TupleReadable.class)));
        ifStatement.condition(condition);

        Block trueBlock = new Block(compilerContext);
        if (projections.isEmpty()) {
            trueBlock
                    .comment("pageBuilder.declarePosition()")
                    .getVariable("pageBuilder")
                    .invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // pageBuilder.getBlockBuilder(0).append(cursor.getDouble(0);
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
                trueBlock.comment("project_%s(cursors..., pageBuilder.getBlockBuilder(%s))", projectionIndex, projectionIndex);
                trueBlock.pushThis();
                for (int channel = 0; channel < channels; channel++) {
                    trueBlock.getVariable("cursor_" + channel);
                }

                // pageBuilder.getBlockBuilder(0)
                trueBlock.getVariable("pageBuilder")
                        .push(projectionIndex)
                        .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

                // project(cursor_0, cursor_1, blockBuilder)
                trueBlock.invokeVirtual(classDefinition.getType(),
                        "project_" + projectionIndex,
                        type(void.class),
                        ImmutableList.<ParameterizedType>builder().addAll(nCopies(channels, type(TupleReadable.class))).add(type(BlockBuilder.class)).build());
            }
        }
        ifStatement.ifTrue(trueBlock);

        forLoopBody.append(ifStatement.build());
        filterAndProjectMethod.getBody().append(forLoop.body(forLoopBody).build());

        //
        //  Verify all cursors ended together
        //

        // checkState(!cursor.advanceNextPosition());
        for (LocalVariableDefinition cursorVariable : cursorVariables) {
            filterAndProjectMethod.getBody()
                    .comment("checkState(not(%s.advanceNextPosition))", cursorVariable.getName())
                    .getVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        filterAndProjectMethod.getBody().ret();
    }

    private void generateFilterAndProjectCursorMethod(ClassDefinition classDefinition,
            List<Expression> projections,
            FieldDefinition operatorStatsField,
            FieldDefinition currentCompletedSizeField)
    {
        MethodDefinition filterAndProjectMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filterAndProjectRowOriented",
                type(void.class),
                arg("cursor", RecordCursor.class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = filterAndProjectMethod.getCompilerContext();

        LocalVariableDefinition completedPositionsVariable = compilerContext.declareVariable(long.class, "completedPositions");
        filterAndProjectMethod.getBody()
                .comment("long completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0L);

        //
        // while loop body
        //

        // while (!pageBuilder.isFull() && cursor.advanceNextPosition())
        LabelNode done = new LabelNode("done");
        WhileLoopBuilder whileLoop = WhileLoop.whileLoopBuilder(compilerContext)
                .condition(new Block(compilerContext)
                        .getVariable("pageBuilder")
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifNotZeroGoto(done)
                        .getVariable("cursor")
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class));

        Block whileLoopBody = new Block(compilerContext);

        whileLoopBody
                .comment("completedPositions++")
                .getVariable(completedPositionsVariable)
                .push(1L)
                .invokeStatic(Operations.class, "add", long.class, long.class, long.class)
                .putVariable(completedPositionsVariable);

        whileLoopBody
                .comment("if (Operations.shouldCheckDone(completedPositions))")
                .append(new IfStatement(compilerContext,
                        new Block(compilerContext)
                                .getVariable(completedPositionsVariable)
                                .invokeStatic(AbstractFilterAndProjectOperator.class, "shouldCheckDoneFlag", boolean.class, long.class),
                        new Block(compilerContext)
                                .comment("if (operatorStats.isDone()) break;")
                                .pushThis()
                                .getField(operatorStatsField)
                                .invokeVirtual(OperatorStats.class, "isDone", boolean.class)
                                .ifTrueGoto(done),
                        NOP
                ));

        // if (filter(cursor))
        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext);
        ifStatement.condition(new Block(compilerContext)
                .pushThis()
                .getVariable("cursor")
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(RecordCursor.class)));

        Block trueBlock = new Block(compilerContext);
        if (projections.isEmpty()) {
            // pageBuilder.declarePosition();
            trueBlock.getVariable("pageBuilder").invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // project_43(cursor_0, cursor_1, pageBuilder.getBlockBuilder(42)));
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
                trueBlock.pushThis();
                trueBlock.getVariable("cursor");

                // pageBuilder.getBlockBuilder(0)
                trueBlock.getVariable("pageBuilder")
                        .push(projectionIndex)
                        .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class);

                // project(cursor_0, cursor_1, blockBuilder)
                trueBlock.invokeVirtual(classDefinition.getType(),
                        "project_" + projectionIndex,
                        type(void.class),
                        type(RecordCursor.class),
                        type(BlockBuilder.class));
            }
        }
        ifStatement.ifTrue(trueBlock);

        whileLoopBody.append(ifStatement.build());
        filterAndProjectMethod.getBody()
                .append(whileLoop.body(whileLoopBody).build())
                .visitLabel(done);

        //
        // Update completed data size in operator stats
        //
        LocalVariableDefinition completedDataSizeVariable = compilerContext.declareVariable(long.class, "completedDataSize");
        filterAndProjectMethod.getBody()
                .comment("long completedDataSize = cursor.getCompletedBytes();")
                .getVariable("cursor")
                .invokeInterface(RecordCursor.class, "getCompletedBytes", long.class)
                .putVariable(completedDataSizeVariable);

        Block shouldUpdateCompletedDataSize = new Block(compilerContext)
                .comment("completedDataSize > this.currentCompletedSize")
                .getVariable(completedDataSizeVariable)
                .pushThis()
                .getField(currentCompletedSizeField)
                .invokeStatic(Operations.class, "greaterThan", boolean.class, long.class, long.class);

        Block updateCompletedDataSize = new Block(compilerContext);

        updateCompletedDataSize
                .comment("operatorStats.addCompletedDataSize(completedDataSize - this.currentCompletedSize);")
                .pushThis()
                .getField(operatorStatsField)
                .getVariable(completedDataSizeVariable)
                .pushThis()
                .getField(currentCompletedSizeField)
                .invokeStatic(Operations.class, "subtract", long.class, long.class, long.class)
                .invokeVirtual(OperatorStats.class, "addCompletedDataSize", void.class, long.class);

        updateCompletedDataSize
                .comment("this.currentCompletedSize = completedDataSize")
                .pushThis()
                .getVariable(completedDataSizeVariable)
                .putField(currentCompletedSizeField);

        filterAndProjectMethod.getBody().append(new IfStatement(compilerContext, shouldUpdateCompletedDataSize, updateCompletedDataSize, NOP));

        //
        // Update completed positions in operator stats
        //
        filterAndProjectMethod
                .getBody()
                .comment("operatorStats.addCompletedPositions(completedPositions);")
                .pushThis()
                .getField(operatorStatsField)
                .getVariable(completedPositionsVariable)
                .invokeVirtual(OperatorStats.class, "addCompletedPositions", void.class, long.class);

        filterAndProjectMethod.getBody().ret();
    }

    private OperatorFactory compileOperatorFactoryClass(List<TupleInfo> tupleInfos, Class<? extends Operator> operatorClass, DynamicClassLoader classLoader)
    {
        Constructor<? extends Operator> constructor;
        try {
            constructor = operatorClass.getConstructor(List.class, Operator.class, Session.class);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProjectOperatorFactory_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(OperatorFactory.class));

        FieldDefinition tupleInfoField = classDefinition.declareField(a(PRIVATE, FINAL), "tupleInfo", type(List.class, TupleInfo.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC), arg("tupleInfos", type(List.class, TupleInfo.class)))
                .getBody()
                .comment("super();")
                .pushThis()
                .invokeConstructor(Object.class)
                .comment("this.tupleInfos = tupleInfos;")
                .pushThis()
                .getVariable("tupleInfos")
                .putField(tupleInfoField)
                .ret();

        // createOperator method
        MethodDefinition applyMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "createOperator",
                type(Operator.class),
                arg("source", Operator.class),
                arg("session", Session.class));

        applyMethod.getBody()
                .comment("return new %s(this.tupleInfo, source, session);", operatorClass.getName())
                .newObject(operatorClass)
                .dup()
                .pushThis()
                .getField(tupleInfoField)
                .getVariable("source")
                .getVariable("session")
                .invokeConstructor(constructor)
                .retObject();

        Class<? extends OperatorFactory> factoryClass = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(OperatorFactory.class);

        try {
            Constructor<? extends OperatorFactory> factoryConstructor = factoryClass.getConstructor(List.class);
            OperatorFactory factory = factoryConstructor.newInstance(tupleInfos);
            return factory;
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private Class<? extends Operator> compileOperatorClass(Class<? extends PageIterator> iteratorClass, DynamicClassLoader classLoader)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProjectOperator_" + CLASS_ID.incrementAndGet()),
                type(AbstractFilterAndProjectOperator.class));

        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("tupleInfos", type(List.class, TupleInfo.class)),
                arg("source", Operator.class),
                arg("session", Session.class))
                .getBody()
                .comment("super(tupleInfos, source);")
                .pushThis()
                .getVariable("tupleInfos")
                .getVariable("source")
                .invokeConstructor(AbstractFilterAndProjectOperator.class, List.class, Operator.class)
                .comment("this.session = session;")
                .pushThis()
                .getVariable("session")
                .putField(sessionField)
                .ret();

        MethodDefinition iteratorMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "iterator",
                type(PageIterator.class),
                arg("source", PageIterator.class),
                arg("operatorStats", OperatorStats.class));

        iteratorMethod.getBody()
                .comment("return new %s(getTupleInfos(), source);", iteratorClass.getName())
                .newObject(iteratorClass)
                .dup()
                .pushThis()
                .invokeInterface(Operator.class, "getTupleInfos", List.class)
                .getVariable("source")
                .pushThis()
                .getField(sessionField)
                .getVariable("operatorStats")
                .invokeConstructor(iteratorClass, Iterable.class, PageIterator.class, Session.class, OperatorStats.class)
                .retObject();

        return defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(Operator.class);
    }

    @VisibleForTesting
    public Function<Session, FilterFunction> internalCompileFilterFunction(Expression expression, Map<Input, Type> inputTypes)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(FilterFunction.class));

        // declare session field
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        //
        // constructor
        //
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC), arg("session", Session.class))
                .getBody()
                .pushThis()
                .invokeConstructor(Object.class)
                .comment("this.session = session;")
                .pushThis()
                .getVariable("session")
                .putField(sessionField)
                .ret();

        //
        // filter function
        //
        MethodDefinition filterMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filter",
                type(boolean.class),
                arg("channels", TupleReadable[].class));

        filterMethod.getBody().pushThis();

        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            filterMethod.getBody()
                    .getVariable("channels")
                    .push(i)
                    .getObjectArrayElement();
        }
        filterMethod.getBody()
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), nCopies(channels, type(TupleReadable.class)));

        filterMethod.getBody().retBoolean();

        //
        // filter method with unrolled channels
        //
        generateFilterMethod(classDefinition, expression, inputTypes, false);
        generateFilterMethod(classDefinition, expression, inputTypes, true);

        //
        // toString method

        classDefinition.declareMethod(new CompilerContext(bootstrapMethod), a(PUBLIC), "toString", type(String.class))
                .getBody()
                .push(toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("filter", expression)
                        .toString())
                .retObject();

        // define the class
        Class<? extends FilterFunction> filterClass = defineClasses(ImmutableList.of(classDefinition), createClassLoader()).values().iterator().next().asSubclass(FilterFunction.class);

        // create instance
        try {
            final Constructor<? extends FilterFunction> constructor = filterClass.getConstructor(Session.class);
            return new Function<Session, FilterFunction>()
            {
                @Override
                public FilterFunction apply(Session session)
                {
                    try {
                        FilterFunction function = constructor.newInstance(session);
                        return function;
                    }
                    catch (Throwable e) {
                        throw Throwables.propagate(e);
                    }
                }
            };
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private void generateFilterMethod(ClassDefinition classDefinition,
            Expression filter,
            Map<Input, Type> inputTypes,
            boolean sourceIsCursor)
    {
        MethodDefinition filterMethod;
        if (sourceIsCursor) {
            filterMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    "filter",
                    type(boolean.class),
                    arg("cursor", RecordCursor.class));
        }
        else {
            filterMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    "filter",
                    type(boolean.class),
                    toTupleReaderParameters(inputTypes));
        }

        filterMethod.comment("Filter: %s", filter.toString());

        filterMethod.getCompilerContext().declareVariable(type(boolean.class), "wasNull");
        Block getSessionByteCode = new Block(filterMethod.getCompilerContext()).pushThis().getField(classDefinition.getType(), "session", type(Session.class));
        TypedByteCodeNode body = new ByteCodeExpressionVisitor(bootstrapFunctionBinder, inputTypes, getSessionByteCode, sourceIsCursor).process(filter, filterMethod.getCompilerContext());

        if (body.getType() == void.class) {
            filterMethod
                    .getBody()
                    .push(false)
                    .retBoolean();
        }
        else {
            LabelNode end = new LabelNode("end");
            filterMethod
                    .getBody()
                    .comment("boolean wasNull = false;")
                    .putVariable("wasNull", false)
                    .append(body.getNode())
                    .getVariable("wasNull")
                    .ifFalseGoto(end)
                    .pop(boolean.class)
                    .push(false)
                    .visitLabel(end)
                    .retBoolean();
        }
    }

    @VisibleForTesting
    public Function<Session, ProjectionFunction> internalCompileProjectionFunction(Expression expression, Map<Input, Type> inputTypes)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("ProjectionFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(ProjectionFunction.class));

        //
        // declare session field
        //
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        //
        // constructor
        //
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC), arg("session", Session.class))
                .getBody()
                .pushThis()
                .invokeConstructor(Object.class)
                .comment("this.session = session;")
                .pushThis()
                .getVariable("session")
                .putField(sessionField)
                .ret();

        //
        // void project(TupleReadable[] channels, BlockBuilder output)
        //
        MethodDefinition projectionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "project",
                type(void.class),
                arg("channels", TupleReadable[].class),
                arg("output", BlockBuilder.class));

        projectionMethod.getBody().pushThis();

        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            projectionMethod.getBody()
                    .getVariable("channels")
                    .push(i)
                    .getObjectArrayElement();
        }

        projectionMethod.getBody().getVariable("output");

        projectionMethod.getBody()
                .invokeVirtual(classDefinition.getType(),
                        "project",
                        type(void.class),
                        ImmutableList.<ParameterizedType>builder().addAll(nCopies(channels, type(TupleReadable.class))).add(type(BlockBuilder.class)).build());
        projectionMethod.getBody().ret();

        //
        // projection with unrolled channels
        //
        Class<?> type = generateProjectMethod(classDefinition, "project", expression, inputTypes, false);
        generateProjectMethod(classDefinition, "project", expression, inputTypes, true);

        //
        // TupleInfo getTupleInfo();
        //
        MethodDefinition getTupleInfoMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "getTupleInfo",
                type(TupleInfo.class));

        if (type == boolean.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_BOOLEAN", type(TupleInfo.class))
                    .retObject();
        }
        // todo remove assumption that void is a long
        else if (type == long.class || type == void.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_LONG", type(TupleInfo.class))
                    .retObject();
        }
        else if (type == double.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_DOUBLE", type(TupleInfo.class))
                    .retObject();
        }
        else if (type == Slice.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_VARBINARY", type(TupleInfo.class))
                    .retObject();
        }
        else {
            throw new IllegalStateException("Type " + type.getName() + "can be output");
        }

        //
        // toString method
        //
        classDefinition.declareMethod(new CompilerContext(bootstrapMethod), a(PUBLIC), "toString", type(String.class))
                .getBody()
                .push(toStringHelper(classDefinition.getType().getJavaClassName())
                        .add("projection", expression)
                        .toString())
                .retObject();

        // define the class
        Class<? extends ProjectionFunction> projectionClass = defineClasses(ImmutableList.of(classDefinition), createClassLoader()).values().iterator().next().asSubclass(ProjectionFunction.class);

        // create instance
        try {
            final Constructor<? extends ProjectionFunction> constructor = projectionClass.getConstructor(Session.class);
            return new Function<Session, ProjectionFunction>()
            {
                @Override
                public ProjectionFunction apply(Session session)
                {
                    try {
                        ProjectionFunction function = constructor.newInstance(session);
                        return function;
                    }
                    catch (Throwable e) {
                        throw Throwables.propagate(e);
                    }

                }
            };
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private Class<?> generateProjectMethod(ClassDefinition classDefinition,
            String methodName,
            Expression projection,
            Map<Input, Type> inputTypes,
            boolean sourceIsCursor)
    {
        MethodDefinition projectionMethod;
        if (sourceIsCursor) {
            projectionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    methodName,
                    type(void.class),
                    arg("cursor", RecordCursor.class),
                    arg("output", BlockBuilder.class));
        }
        else {
            ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
            parameters.addAll(toTupleReaderParameters(inputTypes));
            parameters.add(arg("output", BlockBuilder.class));

            projectionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    methodName,
                    type(void.class),
                    parameters.build());
        }

        projectionMethod.comment("Projection: %s", projection.toString());

        // generate body code
        CompilerContext context = projectionMethod.getCompilerContext();
        context.declareVariable(type(boolean.class), "wasNull");
        Block getSessionByteCode = new Block(context).pushThis().getField(classDefinition.getType(), "session", type(Session.class));
        TypedByteCodeNode body = new ByteCodeExpressionVisitor(bootstrapFunctionBinder, inputTypes, getSessionByteCode, sourceIsCursor).process(projection, context);

        if (body.getType() != void.class) {
            projectionMethod
                    .getBody()
                    .comment("boolean wasNull = false;")
                    .putVariable("wasNull", false)
                    .getVariable("output")
                    .append(body.getNode());

            Block notNullBlock = new Block(context);
            if (body.getType() == boolean.class) {
                notNullBlock
                        .comment("output.append(<booleanStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, boolean.class)
                        .pop();
            }
            else if (body.getType() == long.class) {
                notNullBlock
                        .comment("output.append(<longStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class)
                        .pop();
            }
            else if (body.getType() == double.class) {
                notNullBlock
                        .comment("output.append(<doubleStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, double.class)
                        .pop();
            }
            else if (body.getType() == Slice.class) {
                notNullBlock
                        .comment("output.append(<sliceStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, Slice.class)
                        .pop();
            }
            else {
                throw new UnsupportedOperationException("Type " + body.getType() + " can not be output yet");
            }

            Block nullBlock = new Block(context)
                    .comment("output.appendNull();")
                    .pop(body.getType())
                    .invokeVirtual(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop();

            projectionMethod.getBody()
                    .comment("if the result was null, appendNull; otherwise append the value")
                    .append(new IfStatement(context, new Block(context).getVariable("wasNull"), nullBlock, notNullBlock))
                    .ret();
        }
        else {
            projectionMethod
                    .getBody()
                    .comment("output.appendNull();")
                    .getVariable("output")
                    .invokeVirtual(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .ret();
        }
        return body.getType();
    }

    private static class TypedPageIteratorClass
    {
        private final Class<? extends PageIterator> pageIteratorClass;
        private final List<TupleInfo> tupleInfos;

        private TypedPageIteratorClass(Class<? extends PageIterator> pageIteratorClass, List<TupleInfo> tupleInfos)
        {
            this.pageIteratorClass = pageIteratorClass;
            this.tupleInfos = tupleInfos;
        }

        private Class<? extends PageIterator> getPageIteratorClass()
        {
            return pageIteratorClass;
        }

        private List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }
    }

    private List<NamedParameterDefinition> toTupleReaderParameters(Map<Input, Type> inputTypes)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            parameters.add(arg("channel_" + i, TupleReadable.class));
        }
        return parameters.build();
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

        Map<ParameterizedType, byte[]> byteCodes = new LinkedHashMap<>();
        for (ClassDefinition classDefinition : classDefinitions) {
            ClassWriter cw = new SmartClassWriter(classInfoLoader);
            classDefinition.visit(cw);
            byte[] byteCode = cw.toByteArray();
            if (RUN_ASM_VERIFIER) {
                ClassReader reader = new ClassReader(byteCode);
                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
            }
            byteCodes.put(classDefinition.getType(), byteCode);
        }

        String dumpClassPath = DUMP_CLASS_FILES_TO.get();
        if (dumpClassPath != null) {
            for (Entry<ParameterizedType, byte[]> entry : byteCodes.entrySet()) {
                File file = new File(dumpClassPath, entry.getKey().getClassName() + ".class");
                try {
                    log.debug("ClassFile: " + file.getAbsolutePath());
                    Files.createParentDirs(file);
                    Files.write(entry.getValue(), file);
                }
                catch (IOException e) {
                    log.error(e, "Failed to write generated class file to: %s" + file.getAbsolutePath());
                }
            }
        }
        if (DUMP_BYTE_CODE_RAW) {
            for (byte[] byteCode : byteCodes.values()) {
                ClassReader classReader = new ClassReader(byteCode);
                classReader.accept(new TraceClassVisitor(new PrintWriter(System.err)), ClassReader.SKIP_FRAMES);
            }
        }
        return classLoader.defineClasses(byteCodes);
    }

    private static final class ExpressionCacheKey
    {
        private final Expression expression;
        private final Map<Input, Type> inputTypes;

        private ExpressionCacheKey(Expression expression, Map<Input, Type> inputTypes)
        {
            this.expression = expression;
            this.inputTypes = inputTypes;
        }

        private Expression getExpression()
        {
            return expression;
        }

        private Map<Input, Type> getInputTypes()
        {
            return inputTypes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(expression, inputTypes);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ExpressionCacheKey other = (ExpressionCacheKey) obj;
            return Objects.equal(this.expression, other.expression) && Objects.equal(this.inputTypes, other.inputTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("expression", expression)
                    .add("inputTypes", inputTypes)
                    .toString();
        }
    }

    private static final class OperatorCacheKey
    {
        private final Expression filter;
        private final List<Expression> projections;
        private final Map<Input, Type> inputTypes;

        private OperatorCacheKey(Expression expression, List<Expression> projections, Map<Input, Type> inputTypes)
        {
            this.filter = expression;
            this.projections = ImmutableList.copyOf(projections);
            this.inputTypes = inputTypes;
        }

        private Expression getFilter()
        {
            return filter;
        }

        private List<Expression> getProjections()
        {
            return projections;
        }

        private Map<Input, Type> getInputTypes()
        {
            return inputTypes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(filter, projections, inputTypes);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final OperatorCacheKey other = (OperatorCacheKey) obj;
            return Objects.equal(this.filter, other.filter) && Objects.equal(this.projections, other.projections) && Objects.equal(this.inputTypes, other.inputTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("inputTypes", inputTypes)
                    .toString();
        }
    }
}
