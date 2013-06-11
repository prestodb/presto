/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.gen;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
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
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.control.LookupSwitch.LookupSwitchBuilder;
import com.facebook.presto.byteCode.control.WhileLoop;
import com.facebook.presto.byteCode.control.WhileLoop.WhileLoopBuilder;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.AbstractFilterAndProjectOperator;
import com.facebook.presto.operator.AbstractFilterAndProjectOperator.AbstractFilterAndProjectIterator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.primitives.Primitives;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.OpCodes.L2D;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.byteCode.control.ForLoop.forLoopBuilder;
import static com.facebook.presto.byteCode.control.IfStatement.ifStatementBuilder;
import static com.facebook.presto.byteCode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode.typedByteCodeNode;
import static com.facebook.presto.sql.gen.SliceConstant.sliceConstant;
import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
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
                    .loadVariable("name")
                    .loadVariable("type")
                    .loadVariable("bindingId")
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

        // declare session field
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("tupleInfos", type(Iterable.class, TupleInfo.class)),
                arg("pageIterator", PageIterator.class),
                arg("session", Session.class))
                .getBody()
                .comment("super(tupleInfos, pageIterator);")
                .loadThis()
                .loadVariable("tupleInfos")
                .loadVariable("pageIterator")
                .invokeConstructor(AbstractFilterAndProjectIterator.class, Iterable.class, PageIterator.class)
                .comment("this.session = session;")
                .loadThis()
                .loadVariable("session")
                .putField(classDefinition.getType(), sessionField)
                .ret();

        generateFilterAndProjectCursorMethod(classDefinition, projections);
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
                .loadString(toStringHelper(classDefinition.getType().getJavaClassName())
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
                .loadVariable("blocks")
                .loadConstant(0)
                .loadObjectArray()
                .invokeInterface(com.facebook.presto.block.Block.class, "getPositionCount", int.class)
                .storeVariable(rowsVariable);


        List<LocalVariableDefinition> cursorVariables = new ArrayList<>();
        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            LocalVariableDefinition cursorVariable = compilerContext.declareVariable(BlockCursor.class, "cursor_" + i);
            cursorVariables.add(cursorVariable);
            filterAndProjectMethod.getBody()
                    .comment("BlockCursor %s = blocks[%s].cursor();", cursorVariable.getName(), i)
                    .loadVariable("blocks")
                    .loadConstant(i)
                    .loadObjectArray()
                    .invokeInterface(com.facebook.presto.block.Block.class, "cursor", BlockCursor.class)
                    .storeVariable(cursorVariable);
        }

        //
        // for loop body
        //

        // for (position = 0; position < rows; position++)
        ForLoopBuilder forLoop = forLoopBuilder(compilerContext)
                .comment("for (position = 0; position < rows; position++)")
                .initialize(new Block(compilerContext).loadConstant(0).storeVariable(positionVariable))
                .condition(new Block(compilerContext)
                        .loadVariable(positionVariable)
                        .loadVariable(rowsVariable)
                        .invokeStatic(Operations.class, "lessThan", boolean.class, int.class, int.class))
                .update(new Block(compilerContext).incrementVariable(positionVariable, (byte) 1));

        Block forLoopBody = new Block(compilerContext);

        // cursor.advanceNextPosition()
        for (LocalVariableDefinition cursorVariable : cursorVariables) {
            forLoopBody
                    .comment("checkState(%s.advanceNextPosition());", cursorVariable.getName())
                    .loadVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext)
                .comment("if (filter(cursors...)");
        Block condition = new Block(compilerContext);
        condition.loadThis();
        for (int channel = 0; channel < channels; channel++) {
            condition.loadVariable("cursor_" + channel);
        }
        condition.invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), nCopies(channels, type(TupleReadable.class)));
        ifStatement.condition(condition);

        Block trueBlock = new Block(compilerContext);
        if (projections.isEmpty()) {
            trueBlock
                    .comment("pageBuilder.declarePosition()")
                    .loadVariable("pageBuilder")
                    .invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // pageBuilder.getBlockBuilder(0).append(cursor.getDouble(0);
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
                trueBlock.comment("project_%s(cursors..., pageBuilder.getBlockBuilder(%s))", projectionIndex, projectionIndex);
                trueBlock.loadThis();
                for (int channel = 0; channel < channels; channel++) {
                    trueBlock.loadVariable("cursor_" + channel);
                }

                // pageBuilder.getBlockBuilder(0)
                trueBlock.loadVariable("pageBuilder")
                        .loadConstant(projectionIndex)
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
                    .loadVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        filterAndProjectMethod.getBody().ret();
    }

    private void generateFilterAndProjectCursorMethod(ClassDefinition classDefinition, List<Expression> projections)
    {
        MethodDefinition filterAndProjectMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filterAndProjectRowOriented",
                type(void.class),
                arg("cursor", RecordCursor.class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = filterAndProjectMethod.getCompilerContext();

        //
        // while loop body
        //

        // while (!pageBuilder.isFull() && cursor.advanceNextPosition())
        LabelNode done = new LabelNode("done");
        WhileLoopBuilder whileLoop = WhileLoop.whileLoopBuilder(compilerContext)
                .condition(new Block(compilerContext)
                        .loadVariable("pageBuilder")
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifNotZeroGoto(done)
                        .loadVariable("cursor")
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class));

        Block whileLoopBody = new Block(compilerContext);

        // if (filter(cursor))
        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext);
        ifStatement.condition(new Block(compilerContext)
                .loadThis()
                .loadVariable("cursor")
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(RecordCursor.class)));

        Block trueBlock = new Block(compilerContext);
        if (projections.isEmpty()) {
            // pageBuilder.declarePosition();
            trueBlock.loadVariable("pageBuilder").invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // project_43(cursor_0, cursor_1, pageBuilder.getBlockBuilder(42)));
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
                trueBlock.loadThis();
                trueBlock.loadVariable("cursor");

                // pageBuilder.getBlockBuilder(0)
                trueBlock.loadVariable("pageBuilder")
                        .loadConstant(projectionIndex)
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
                .loadThis()
                .invokeConstructor(Object.class)
                .comment("this.tupleInfos = tupleInfos;")
                .loadThis()
                .loadVariable("tupleInfos")
                .putField(classDefinition.getType(), tupleInfoField)
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
                .loadThis()
                .getField(classDefinition.getType(), tupleInfoField)
                .loadVariable("source")
                .loadVariable("session")
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
                .loadThis()
                .loadVariable("tupleInfos")
                .loadVariable("source")
                .invokeConstructor(AbstractFilterAndProjectOperator.class, List.class, Operator.class)
                .comment("this.session = session;")
                .loadThis()
                .loadVariable("session")
                .putField(classDefinition.getType(), sessionField)
                .ret();

        MethodDefinition iteratorMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "iterator",
                type(PageIterator.class),
                arg("source", PageIterator.class));

        iteratorMethod.getBody()
                .comment("return new %s(getTupleInfos(), source);", iteratorClass.getName())
                .newObject(iteratorClass)
                .dup()
                .loadThis()
                .invokeInterface(Operator.class, "getTupleInfos", List.class)
                .loadVariable("source")
                .loadThis()
                .getField(classDefinition.getType(), sessionField)
                .invokeConstructor(iteratorClass, Iterable.class, PageIterator.class, Session.class)
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
                .loadThis()
                .invokeConstructor(Object.class)
                .comment("this.session = session;")
                .loadThis()
                .loadVariable("session")
                .putField(classDefinition.getType(), sessionField)
                .ret();

        //
        // filter function
        //
        MethodDefinition filterMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filter",
                type(boolean.class),
                arg("channels", TupleReadable[].class));

        filterMethod.getBody().loadThis();

        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            filterMethod.getBody()
                    .loadVariable("channels")
                    .loadConstant(i)
                    .loadObjectArray();
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
                .loadString(toStringHelper(classDefinition.getType().getJavaClassName())
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
        Block getSessionByteCode = new Block(filterMethod.getCompilerContext()).loadThis().getField(classDefinition.getType(), "session", type(Session.class));
        TypedByteCodeNode body = new Visitor(bootstrapFunctionBinder, inputTypes, getSessionByteCode, sourceIsCursor).process(filter, filterMethod.getCompilerContext());

        if (body.type == void.class) {
            filterMethod
                    .getBody()
                    .loadConstant(false)
                    .retBoolean();
        }
        else {
            LabelNode end = new LabelNode("end");
            filterMethod
                    .getBody()
                    .comment("boolean wasNull = false;")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .append(body.node)
                    .loadVariable("wasNull")
                    .ifZeroGoto(end)
                    .pop(boolean.class)
                    .loadConstant(false)
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
                .loadThis()
                .invokeConstructor(Object.class)
                .comment("this.session = session;")
                .loadThis()
                .loadVariable("session")
                .putField(classDefinition.getType(), sessionField)
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

        projectionMethod.getBody().loadThis();

        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            projectionMethod.getBody()
                    .loadVariable("channels")
                    .loadConstant(i)
                    .loadObjectArray();
        }

        projectionMethod.getBody().loadVariable("output");

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
                .loadString(toStringHelper(classDefinition.getType().getJavaClassName())
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
        Block getSessionByteCode = new Block(context).loadThis().getField(classDefinition.getType(), "session", type(Session.class));
        TypedByteCodeNode body = new Visitor(bootstrapFunctionBinder, inputTypes, getSessionByteCode, sourceIsCursor).process(projection, context);

        if (body.type != void.class) {
            projectionMethod
                    .getBody()
                    .comment("boolean wasNull = false;")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .loadVariable("output")
                    .append(body.node);

            Block notNullBlock = new Block(context);
            if (body.type == boolean.class) {
                notNullBlock
                        .comment("output.append(<booleanStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, boolean.class);
            }
            else if (body.type == long.class) {
                notNullBlock
                        .comment("output.append(<longStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class);
            }
            else if (body.type == double.class) {
                notNullBlock
                        .comment("output.append(<doubleStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, double.class);
            }
            else if (body.type == Slice.class) {
                notNullBlock
                        .comment("output.append(<sliceStackValue>);")
                        .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, Slice.class);
            }
            else {
                throw new UnsupportedOperationException("Type " + body.type + " can not be output yet");
            }

            Block nullBlock = new Block(context)
                    .comment("output.appendNull();")
                    .pop(body.type)
                    .invokeVirtual(BlockBuilder.class, "appendNull", BlockBuilder.class);

            projectionMethod.getBody()
                    .comment("if the result was null, appendNull; otherwise append the value")
                    .append(new IfStatement(context, new Block(context).loadVariable("wasNull"), nullBlock, notNullBlock))
                    .ret();
        }
        else {
            projectionMethod
                    .getBody()
                    .comment("output.appendNull();")
                    .loadVariable("output")
                    .invokeVirtual(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .ret();
        }
        return body.type;
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

    public static class TypedByteCodeNode
    {
        public static TypedByteCodeNode typedByteCodeNode(ByteCodeNode node, Class<?> type)
        {
            return new TypedByteCodeNode(node, type);
        }

        private final ByteCodeNode node;
        private final Class<?> type;

        private TypedByteCodeNode(ByteCodeNode node, Class<?> type)
        {
            this.node = node;
            this.type = type;
        }

        public ByteCodeNode getNode()
        {
            return node;
        }

        public Class<?> getType()
        {
            return type;
        }
    }

    private static class Visitor
            extends AstVisitor<TypedByteCodeNode, CompilerContext>
    {
        private final BootstrapFunctionBinder bootstrapFunctionBinder;
        private final Map<Input, Type> inputTypes;
        private final ByteCodeNode getSessionByteCode;
        private final boolean sourceIsCursor;

        public Visitor(BootstrapFunctionBinder bootstrapFunctionBinder, Map<Input, Type> inputTypes, ByteCodeNode getSessionByteCode, boolean sourceIsCursor)
        {
            this.bootstrapFunctionBinder = bootstrapFunctionBinder;
            this.inputTypes = inputTypes;
            this.getSessionByteCode = getSessionByteCode;
            this.sourceIsCursor = sourceIsCursor;
        }

        @Override
        protected TypedByteCodeNode visitBooleanLiteral(BooleanLiteral node, CompilerContext context)
        {
            return typedByteCodeNode(loadBoolean(node.getValue()), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitLongLiteral(LongLiteral node, CompilerContext context)
        {
            return typedByteCodeNode(loadLong(node.getValue()), long.class);
        }

        @Override
        protected TypedByteCodeNode visitDoubleLiteral(DoubleLiteral node, CompilerContext context)
        {
            return typedByteCodeNode(loadDouble(node.getValue()), double.class);
        }

        @Override
        protected TypedByteCodeNode visitStringLiteral(StringLiteral node, CompilerContext context)
        {
            return typedByteCodeNode(sliceConstant(node.getSlice()), Slice.class);
        }

        @Override
        protected TypedByteCodeNode visitNullLiteral(NullLiteral node, CompilerContext context)
        {
            // todo this should be the real type of the expression
            return typedByteCodeNode(new Block(context).loadConstant(true).storeVariable("wasNull"), void.class);
        }

        @Override
        public TypedByteCodeNode visitInputReference(InputReference node, CompilerContext context)
        {
            Input input = node.getInput();
            int channel = input.getChannel();
            Type type = inputTypes.get(input);
            checkState(type != null, "No type for input %s", input);

            if (sourceIsCursor) {
                int field = input.getField();
                Block isNullCheck = new Block(context)
                        .setDescription(format("cursor.get%s(%d)", type, field))
                        .loadVariable("cursor")
                        .loadConstant(field)
                        .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);


                switch (type) {
                    case BOOLEAN: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(boolean.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("cursor")
                                .loadConstant(channel)
                                .invokeInterface(RecordCursor.class, "getBoolean", boolean.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), boolean.class);
                    }
                    case LONG: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(long.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("cursor")
                                .loadConstant(channel)
                                .invokeInterface(RecordCursor.class, "getLong", long.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), long.class);
                    }
                    case DOUBLE: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(double.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("cursor")
                                .loadConstant(channel)
                                .invokeInterface(RecordCursor.class, "getDouble", double.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), double.class);
                    }
                    case STRING: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(Slice.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("cursor")
                                .loadConstant(channel)
                                .invokeInterface(RecordCursor.class, "getString", byte[].class, int.class)
                                .invokeStatic(Slices.class, "wrappedBuffer", Slice.class, byte[].class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), Slice.class);
                    }
                    default:
                        throw new UnsupportedOperationException("not yet implemented: " + type);
                }
            }
            else {
                int field = input.getField();
                Block isNullCheck = new Block(context)
                        .setDescription(format("channel_%d.get%s(%d)", channel, type, field))
                        .loadVariable("channel_" + channel)
                        .loadConstant(field)
                        .invokeInterface(TupleReadable.class, "isNull", boolean.class, int.class);

                switch (type) {
                    case BOOLEAN: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(boolean.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("channel_" + channel)
                                .loadConstant(field)
                                .invokeInterface(TupleReadable.class, "getBoolean", boolean.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), boolean.class);
                    }
                    case LONG: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(long.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("channel_" + channel)
                                .loadConstant(field)
                                .invokeInterface(TupleReadable.class, "getLong", long.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), long.class);
                    }
                    case DOUBLE: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(double.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("channel_" + channel)
                                .loadConstant(field)
                                .invokeInterface(TupleReadable.class, "getDouble", double.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), double.class);
                    }
                    case STRING: {
                        Block isNull = new Block(context)
                                .loadConstant(true)
                                .storeVariable("wasNull")
                                .loadJavaDefault(Slice.class);

                        Block isNotNull = new Block(context)
                                .loadVariable("channel_" + channel)
                                .loadConstant(field)
                                .invokeInterface(TupleReadable.class, "getSlice", Slice.class, int.class);

                        return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), Slice.class);
                    }
                    default:
                        throw new UnsupportedOperationException("not yet implemented: " + type);
                }
            }
        }

        @Override
        protected TypedByteCodeNode visitCurrentTime(CurrentTime node, CompilerContext context)
        {
            return visitFunctionCall(new FunctionCall(new QualifiedName("now"), ImmutableList.<Expression>of()), context);
        }

        @Override
        protected TypedByteCodeNode visitFunctionCall(FunctionCall node, CompilerContext context)
        {
            List<TypedByteCodeNode> arguments = new ArrayList<>();
            for (Expression argument : node.getArguments()) {
                TypedByteCodeNode typedByteCodeNode = process(argument, context);
                if (typedByteCodeNode.type == void.class) {
                    return typedByteCodeNode;
                }
                arguments.add(typedByteCodeNode);
            }

            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(node.getName(), getSessionByteCode, arguments);
            return visitFunctionBinding(context, functionBinding, node.toString());
        }

        @Override
        protected TypedByteCodeNode visitExtract(Extract node, CompilerContext context)
        {
            TypedByteCodeNode expression = process(node.getExpression(), context);
            if (expression.type == void.class) {
                return expression;
            }

            if (node.getField() == Extract.Field.TIMEZONE_HOUR || node.getField() == Extract.Field.TIMEZONE_MINUTE) {
                // TODO: we assume all times are UTC for now
                return new TypedByteCodeNode(new Block(context).append(expression.node).pop(long.class).loadConstant(0L), long.class);
            }

            QualifiedName functionName = QualifiedName.of(node.getField().name().toLowerCase());
            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(functionName, getSessionByteCode, ImmutableList.of(expression));
            return visitFunctionBinding(context, functionBinding, node.toString());
        }

        @Override
        protected TypedByteCodeNode visitLikePredicate(LikePredicate node, CompilerContext context)
        {
            ImmutableList<Expression> expressions;
            if (node.getEscape() != null) {
                expressions = ImmutableList.of(node.getValue(), node.getPattern(), node.getEscape());
            }
            else {
                expressions = ImmutableList.of(node.getValue(), node.getPattern());
            }

            List<TypedByteCodeNode> arguments = new ArrayList<>();
            for (Expression argument : expressions) {
                TypedByteCodeNode typedByteCodeNode = process(argument, context);
                if (typedByteCodeNode.type == void.class) {
                    return typedByteCodeNode;
                }
                arguments.add(typedByteCodeNode);
            }

            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction("like", getSessionByteCode, arguments, new LikeFunctionBinder());
            return visitFunctionBinding(context, functionBinding, node.toString());
        }

        private TypedByteCodeNode visitFunctionBinding(CompilerContext context, FunctionBinding functionBinding, String comment)
        {
            List<TypedByteCodeNode> arguments = functionBinding.getArguments();
            MethodType methodType = functionBinding.getCallSite().type();
            Class<?> unboxedReturnType = Primitives.unwrap(methodType.returnType());

            LabelNode end = new LabelNode("end");
            Block block = new Block(context)
                    .setDescription("invoke")
                    .comment(comment);
            ArrayList<Class<?>> stackTypes = new ArrayList<>();
            for (int i = 0; i < arguments.size(); i++) {
                TypedByteCodeNode argument = arguments.get(i);
                Class<?> argumentType = methodType.parameterList().get(i);
                block.append(coerceToType(context, argument, argumentType).node);

                stackTypes.add(argument.type);
                block.append(ifWasNullPopAndGoto(context, end, unboxedReturnType, Lists.reverse(stackTypes)));
            }
            block.invokeDynamic(functionBinding.getName(), methodType, functionBinding.getBindingId());

            if (functionBinding.isNullable()) {
                if (unboxedReturnType.isPrimitive()) {
                    LabelNode notNull = new LabelNode("notNull");
                    block.dup(methodType.returnType())
                            .ifNotNullGoto(notNull)
                            .loadConstant(true)
                            .storeVariable("wasNull")
                            .comment("swap boxed null with unboxed default")
                            .pop(methodType.returnType())
                            .loadJavaDefault(unboxedReturnType)
                            .gotoLabel(end)
                            .visitLabel(notNull)
                            .append(unboxPrimitive(context, unboxedReturnType));
                }
                else {
                    block.dup(methodType.returnType())
                            .ifNotNullGoto(end)
                            .loadConstant(true)
                            .storeVariable("wasNull");
                }
            }
            block.visitLabel(end);

            return typedByteCodeNode(block, unboxedReturnType);
        }

        private static ByteCodeNode unboxPrimitive(CompilerContext context, Class<?> unboxedType)
        {
            Block block = new Block(context).comment("unbox primitive");
            if (unboxedType == long.class) {
                return block.invokeVirtual(Long.class, "longValue", long.class);
            }
            if (unboxedType == double.class) {
                return block.invokeVirtual(Double.class, "doubleValue", double.class);
            }
            if (unboxedType == boolean.class) {
                return block.invokeVirtual(Boolean.class, "booleanValue", boolean.class);
            }
            throw new UnsupportedOperationException("not yet implemented: " + unboxedType);
        }

        @Override
        public TypedByteCodeNode visitCast(Cast node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getExpression(), context);

            Block block = new Block(context).comment(node.toString());
            block.append(value.node);

            if (value.type == void.class) {
                switch (node.getType()) {
                    case "BOOLEAN":
                        block.loadJavaDefault(boolean.class);
                        return typedByteCodeNode(block, boolean.class);
                    case "BIGINT":
                        block.loadJavaDefault(long.class);
                        return typedByteCodeNode(block, long.class);
                    case "DOUBLE":
                        block.loadJavaDefault(double.class);
                        return typedByteCodeNode(block, double.class);
                    case "VARCHAR":
                        block.loadJavaDefault(Slice.class);
                        return typedByteCodeNode(block, Slice.class);
                }
            }
            else {
                LabelNode end = new LabelNode("end");
                switch (node.getType()) {
                    case "BOOLEAN":
                        block.append(ifWasNullPopAndGoto(context, end, boolean.class, value.type));
                        block.invokeStatic(Operations.class, "castToBoolean", boolean.class, value.type);
                        return typedByteCodeNode(block.visitLabel(end), boolean.class);
                    case "BIGINT":
                        block.append(ifWasNullPopAndGoto(context, end, long.class, value.type));
                        block.invokeStatic(Operations.class, "castToLong", long.class, value.type);
                        return typedByteCodeNode(block.visitLabel(end), long.class);
                    case "DOUBLE":
                        block.append(ifWasNullPopAndGoto(context, end, double.class, value.type));
                        block.invokeStatic(Operations.class, "castToDouble", double.class, value.type);
                        return typedByteCodeNode(block.visitLabel(end), double.class);
                    case "VARCHAR":
                        block.append(ifWasNullPopAndGoto(context, end, Slice.class, value.type));
                        block.invokeStatic(Operations.class, "castToSlice", Slice.class, value.type);
                        return typedByteCodeNode(block.visitLabel(end), Slice.class);
                }
            }
            throw new UnsupportedOperationException("Unsupported type: " + node.getType());
        }

        @Override
        protected TypedByteCodeNode visitArithmeticExpression(ArithmeticExpression node, CompilerContext context)
        {
            TypedByteCodeNode left = process(node.getLeft(), context);
            if (left.type == void.class) {
                return left;
            }

            TypedByteCodeNode right = process(node.getRight(), context);
            if (right.type == void.class) {
                return right;
            }

            Class<?> type = getType(left, right);
            if (!isNumber(type)) {
                throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }

            Block block = new Block(context).comment(node.toString());
            LabelNode end = new LabelNode("end");

            block.append(coerceToType(context, left, type).node);
            block.append(ifWasNullPopAndGoto(context, end, type, left.type));

            block.append(coerceToType(context, right, type).node);
            block.append(ifWasNullPopAndGoto(context, end, type, type, right.type));

            switch (node.getType()) {
                case ADD:
                    block.invokeStatic(Operations.class, "add", type, type, type);
                    break;
                case SUBTRACT:
                    block.invokeStatic(Operations.class, "subtract", type, type, type);
                    break;
                case MULTIPLY:
                    block.invokeStatic(Operations.class, "multiply", type, type, type);
                    break;
                case DIVIDE:
                    block.invokeStatic(Operations.class, "divide", type, type, type);
                    break;
                case MODULUS:
                    block.invokeStatic(Operations.class, "modulus", type, type, type);
                    break;
                default:
                    throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }
            return typedByteCodeNode(block.visitLabel(end), type);
        }

        @Override
        protected TypedByteCodeNode visitNegativeExpression(NegativeExpression node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return value;
            }

            if (!isNumber(value.type)) {
                throw new UnsupportedOperationException(format("not yet implemented: negate(%s)", value.type));
            }

            // simple single op so there is no reason to do a null check
            Block block = new Block(context)
                    .comment(node.toString())
                    .append(value.node)
                    .invokeStatic(Operations.class, "negate", value.type, value.type);

            return typedByteCodeNode(block, value.type);
        }

        @Override
        protected TypedByteCodeNode visitLogicalBinaryExpression(LogicalBinaryExpression node, CompilerContext context)
        {
            TypedByteCodeNode left = process(node.getLeft(), context);
            if (left.type == void.class) {
                left = coerceToType(context, left, boolean.class);
            }
            Preconditions.checkState(left.type == boolean.class, "Expected logical binary expression left value to be a boolean but is a %s: %s", left.type.getName(), node);

            TypedByteCodeNode right = process(node.getRight(), context);
            if (right.type == void.class) {
                right = coerceToType(context, right, boolean.class);
            }
            Preconditions.checkState(right.type == boolean.class, "Expected logical binary expression right value to be a boolean but is a %s: %s", right.type.getName(), node);

            switch (node.getType()) {
                case AND: {
                    return visitAnd(context, left, right, node.toString());

                }
                case OR: {
                    return visitOr(context, left, right, node.toString());
                }
            }
            throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
        }

        private TypedByteCodeNode visitAnd(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right, String comment)
        {
            Block block = new Block(context)
                    .comment(comment)
                    .setDescription("AND");

            block.append(left.node);

            IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                    .comment("if left wasNull...")
                    .condition(new Block(context).loadVariable("wasNull"));

            LabelNode end = new LabelNode("end");
            ifLeftIsNull.ifTrue(new Block(context)
                    .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .pop(left.type) // discard left value
                    .loadConstant(true));

            LabelNode leftIsTrue = new LabelNode("leftIsTrue");
            ifLeftIsNull.ifFalse(new Block(context)
                    .comment("if left is false, push false, and goto end")
                    .ifNotZeroGoto(leftIsTrue)
                    .loadConstant(false)
                    .gotoLabel(end)
                    .comment("left was true; push left null flag on the stack (false)")
                    .visitLabel(leftIsTrue)
                    .loadConstant(false));

            block.append(ifLeftIsNull.build());

            // At this point we know the left expression was either NULL or TRUE.  The stack contains a single boolean
            // value for this expression which indicates if the left value was NULL.

            // eval right!
            block.append(right.node);

            IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                    .comment("if right wasNull...")
                    .condition(new Block(context).loadVariable("wasNull"));

            ifRightIsNull.ifTrue(new Block(context)
                    .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                    // this leaves a single boolean on the stack which is ignored since the value in NULL
                    .pop(right.type));

            LabelNode rightIsTrue = new LabelNode("rightIsTrue");
            ifRightIsNull.ifFalse(new Block(context)
                    .comment("if right is false, pop left null flag off stack, push false and goto end")
                    .ifNotZeroGoto(rightIsTrue)
                    .pop(boolean.class)
                    .loadConstant(false)
                    .gotoLabel(end)
                    .comment("right was true; store left null flag (on stack) in wasNull variable, and push true")
                    .visitLabel(rightIsTrue)
                    .storeVariable("wasNull")
                    .loadConstant(true));

            block.append(ifRightIsNull.build())
                    .visitLabel(end);

            return typedByteCodeNode(block, boolean.class);
        }

        private TypedByteCodeNode visitOr(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right, String comment)
        {
            Block block = new Block(context)
                    .comment(comment)
                    .setDescription("OR");

            block.append(left.node);

            IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                    .comment("if left wasNull...")
                    .condition(new Block(context).loadVariable("wasNull"));

            LabelNode end = new LabelNode("end");
            ifLeftIsNull.ifTrue(new Block(context)
                    .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .pop(left.type) // discard left value
                    .loadConstant(true));

            LabelNode leftIsFalse = new LabelNode("leftIsFalse");
            ifLeftIsNull.ifFalse(new Block(context)
                    .comment("if left is true, push true, and goto end")
                    .ifZeroGoto(leftIsFalse)
                    .loadConstant(true)
                    .gotoLabel(end)
                    .comment("left was false; push left null flag on the stack (false)")
                    .visitLabel(leftIsFalse)
                    .loadConstant(false));

            block.append(ifLeftIsNull.build());

            // At this point we know the left expression was either NULL or FALSE.  The stack contains a single boolean
            // value for this expression which indicates if the left value was NULL.

            // eval right!
            block.append(right.node);

            IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                    .comment("if right wasNull...")
                    .condition(new Block(context).loadVariable("wasNull"));

            ifRightIsNull.ifTrue(new Block(context)
                    .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                    // this leaves a single boolean on the stack which is ignored since the value in NULL
                    .pop(right.type));

            LabelNode rightIsTrue = new LabelNode("rightIsTrue");
            ifRightIsNull.ifFalse(new Block(context)
                    .comment("if right is true, pop left null flag off stack, push true and goto end")
                    .ifZeroGoto(rightIsTrue)
                    .pop(boolean.class)
                    .loadConstant(true)
                    .gotoLabel(end)
                    .comment("right was false; store left null flag (on stack) in wasNull variable, and push false")
                    .visitLabel(rightIsTrue)
                    .storeVariable("wasNull")
                    .loadConstant(false));

            block.append(ifRightIsNull.build())
                    .visitLabel(end);

            return typedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitNotExpression(NotExpression node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return value;
            }

            Preconditions.checkState(value.type == boolean.class);
            // simple single op so there is no reason to do a null check
            return typedByteCodeNode(new Block(context)
                    .comment(node.toString())
                    .append(value.node)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitComparisonExpression(ComparisonExpression node, CompilerContext context)
        {
            // distinct requires special null handling rules
            if (node.getType() == ComparisonExpression.Type.IS_DISTINCT_FROM) {
                return visitIsDistinctFrom(node, context);
            }

            TypedByteCodeNode left = process(node.getLeft(), context);
            if (left.type == void.class) {
                return left;
            }

            TypedByteCodeNode right = process(node.getRight(), context);
            if (right.type == void.class) {
                return right;
            }

            Class<?> type = getType(left, right);

            String function;
            switch (node.getType()) {
                case EQUAL:
                    function = "equal";
                    break;
                case NOT_EQUAL:
                    function = "notEqual";
                    break;
                case LESS_THAN:
                    checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type);
                    function = "lessThan";
                    break;
                case LESS_THAN_OR_EQUAL:
                    checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type);
                    function = "lessThanOrEqual";
                    break;
                case GREATER_THAN:
                    checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type);
                    function = "greaterThan";
                    break;
                case GREATER_THAN_OR_EQUAL:
                    checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type);
                    function = "greaterThanOrEqual";
                    break;
                default:
                    throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }

            LabelNode end = new LabelNode("end");
            Block block = new Block(context)
                    .comment(node.toString());

            block.append(coerceToType(context, left, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, left.type));

            block.append(coerceToType(context, right, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, right.type));

            block.invokeStatic(Operations.class, function, boolean.class, type, type);
            return typedByteCodeNode(block.visitLabel(end), boolean.class);
        }

        private TypedByteCodeNode visitIsDistinctFrom(ComparisonExpression node, CompilerContext context)
        {
            TypedByteCodeNode left = process(node.getLeft(), context);
            TypedByteCodeNode right = process(node.getRight(), context);

            Class<?> type = getType(left, right);
            if (type == void.class) {
                // both left and right are literal nulls, which are not "distinct from" each other
                return new TypedByteCodeNode(loadBoolean(false), boolean.class);
            }

            Block block = new Block(context)
                    .comment(node.toString())
                    .comment("left")
                    .append(coerceToType(context, left, type).node)
                    .loadVariable("wasNull")
                    .comment("clear was null")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .comment("right")
                    .append(coerceToType(context, right, type).node)
                    .loadVariable("wasNull")
                    .comment("clear was null")
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .invokeStatic(Operations.class, "isDistinctFrom", boolean.class, type, boolean.class, type, boolean.class);

            return new TypedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitBetweenPredicate(BetweenPredicate node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return value;
            }

            TypedByteCodeNode min = process(node.getMin(), context);
            if (min.type == void.class) {
                return min;
            }

            TypedByteCodeNode max = process(node.getMax(), context);
            if (max.type == void.class) {
                return max;
            }

            Class<?> type = getType(value, min, max);

            LabelNode end = new LabelNode("end");
            Block block = new Block(context)
                    .comment(node.toString());

            block.append(coerceToType(context, value, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, type));

            block.append(coerceToType(context, min, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, type));

            block.append(coerceToType(context, max, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, type, type));

            block.invokeStatic(Operations.class, "between", boolean.class, type, type, type);
            return typedByteCodeNode(block.visitLabel(end), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitIsNotNullPredicate(IsNotNullPredicate node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return typedByteCodeNode(loadBoolean(false), boolean.class);

            }

            // evaluate the expression, pop the produced value, load the null flag, and invert it
            Block block = new Block(context)
                    .comment(node.toString())
                    .append(value.node)
                    .pop(value.type)
                    .loadVariable("wasNull")
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class);

            // clear the null flag
            block.loadConstant(false)
                    .storeVariable("wasNull");

            return typedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitIsNullPredicate(IsNullPredicate node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return typedByteCodeNode(loadBoolean(true), boolean.class);
            }

            // evaluate the expression, pop the produced value, and load the null flag
            Block block = new Block(context)
                    .comment(node.toString())
                    .append(value.node)
                    .pop(value.type)
                    .loadVariable("wasNull");

            // clear the null flag
            block.loadConstant(false)
                    .storeVariable("wasNull");

            return typedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitIfExpression(IfExpression node, CompilerContext context)
        {
            TypedByteCodeNode conditionValue = process(node.getCondition(), context);
            TypedByteCodeNode trueValue = process(node.getTrueValue(), context);
            TypedByteCodeNode falseValue = process(node.getFalseValue().or(new NullLiteral()), context);

            if (conditionValue.type == void.class) {
                return falseValue;
            }
            Preconditions.checkState(conditionValue.type == boolean.class);

            // clear null flag after evaluating condition
            Block condition = new Block(context)
                    .comment(node.toString())
                    .append(conditionValue.node)
                    .loadConstant(false)
                    .storeVariable("wasNull");

            Class<?> type = getType(trueValue, falseValue);
            if (type == void.class) {
                // both true and false are null literal
                return trueValue;
            }

            trueValue = coerceToType(context, trueValue, type);
            falseValue = coerceToType(context, falseValue, type);

            return typedByteCodeNode(new IfStatement(context, condition, trueValue.node, falseValue.node), type);
        }

        @Override
        protected TypedByteCodeNode visitSearchedCaseExpression(SearchedCaseExpression node, final CompilerContext context)
        {
            TypedByteCodeNode elseValue;
            if (node.getDefaultValue() != null) {
                elseValue = process(node.getDefaultValue(), context);
            }
            else {
                elseValue = process(new NullLiteral(), context);
            }

            List<TypedWhenClause> whenClauses = ImmutableList.copyOf(transform(node.getWhenClauses(), new Function<WhenClause, TypedWhenClause>()
            {
                @Override
                public TypedWhenClause apply(WhenClause whenClause)
                {
                    return new TypedWhenClause(context, whenClause);
                }
            }));

            Class<?> type = getType(ImmutableList.<TypedByteCodeNode>builder().addAll(transform(whenClauses, whenValueGetter())).add(elseValue).build());

            elseValue = coerceToType(context, elseValue, type);
            // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
            for (TypedWhenClause whenClause : Lists.reverse(new ArrayList<>(whenClauses))) {
                if (whenClause.condition.type == void.class) {
                    continue;
                }
                Preconditions.checkState(whenClause.condition.type == boolean.class);

                // clear null flag after evaluating condition
                Block condition = new Block(context)
                        .append(whenClause.condition.node)
                        .loadConstant(false)
                        .storeVariable("wasNull");

                elseValue = typedByteCodeNode(new IfStatement(context, condition, coerceToType(context, whenClause.value, type).node, elseValue.node), type);
            }

            return elseValue;
        }

        @Override
        protected TypedByteCodeNode visitSimpleCaseExpression(SimpleCaseExpression node, final CompilerContext context)
        {
            // process value, else, and all when clauses
            TypedByteCodeNode value = process(node.getOperand(), context);
            TypedByteCodeNode elseValue;
            if (node.getDefaultValue() != null) {
                elseValue = process(node.getDefaultValue(), context);
            }
            else {
                elseValue = process(new NullLiteral(), context);
            }
            List<TypedWhenClause> whenClauses = ImmutableList.copyOf(transform(node.getWhenClauses(), new Function<WhenClause, TypedWhenClause>()
            {
                @Override
                public TypedWhenClause apply(WhenClause whenClause)
                {
                    return new TypedWhenClause(context, whenClause);
                }
            }));

            // determine the type of the value and result
            Class<?> valueType = getType(ImmutableList.<TypedByteCodeNode>builder().addAll(transform(whenClauses, whenConditionGetter())).add(value).build());
            Class<?> resultType = getType(ImmutableList.<TypedByteCodeNode>builder().addAll(transform(whenClauses, whenValueGetter())).add(elseValue).build());

            if (value.type == void.class) {
                return coerceToType(context, elseValue, resultType);
            }

            // evaluate the value and store it in a variable
            LabelNode nullValue = new LabelNode("nullCondition");
            Variable tempVariable = context.createTempVariable(valueType);
            Block block = new Block(context)
                    .append(coerceToType(context, value, valueType).node)
                    .append(ifWasNullClearPopAndGoto(context, nullValue, void.class, valueType))
                    .storeVariable(tempVariable.getLocalVariableDefinition());

            // build the statements
            elseValue = typedByteCodeNode(new Block(context).visitLabel(nullValue).append(coerceToType(context, elseValue, resultType).node), resultType);
            // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
            for (TypedWhenClause whenClause : Lists.reverse(new ArrayList<>(whenClauses))) {
                LabelNode nullCondition = new LabelNode("nullCondition");
                Block condition = new Block(context)
                        .append(coerceToType(context, whenClause.condition, valueType).node)
                        .append(ifWasNullPopAndGoto(context, nullCondition, boolean.class, valueType))
                        .loadVariable(tempVariable.getLocalVariableDefinition())
                        .invokeStatic(Operations.class, "equal", boolean.class, valueType, valueType)
                        .visitLabel(nullCondition)
                        .loadConstant(false)
                        .storeVariable("wasNull");

                elseValue = typedByteCodeNode(new IfStatement(context,
                        format("when %s", whenClause),
                        condition,
                        coerceToType(context, whenClause.value, resultType).node,
                        elseValue.node), resultType);
            }

            return typedByteCodeNode(block.append(elseValue.node), resultType);
        }

        @Override
        protected TypedByteCodeNode visitNullIfExpression(NullIfExpression node, CompilerContext context)
        {
            TypedByteCodeNode first = process(node.getFirst(), context);
            TypedByteCodeNode second = process(node.getSecond(), context);
            if (first.type == void.class) {
                return first;
            }

            Class<?> comparisonType = getType(first, second);

            LabelNode notMatch = new LabelNode("notMatch");
            Block block = new Block(context)
                    .comment(node.toString())
                    .append(first.node)
                    .append(ifWasNullPopAndGoto(context, notMatch, void.class))
                    .append(coerceToType(context, typedByteCodeNode(new Block(context).dup(first.type), first.type), comparisonType).node)
                    .append(coerceToType(context, second, comparisonType).node)
                    .append(ifWasNullClearPopAndGoto(context, notMatch, void.class, comparisonType, comparisonType));

            Block conditionBlock = new Block(context)
                    .invokeStatic(Operations.class, "equal", boolean.class, comparisonType, comparisonType);

            Block trueBlock = new Block(context)
                    .loadConstant(true)
                    .storeVariable("wasNull")
                    .pop(first.type)
                    .loadJavaDefault(first.type);

            block.append(new IfStatement(context, conditionBlock, trueBlock, notMatch));

            return typedByteCodeNode(block, first.type);
        }

        @Override
        protected TypedByteCodeNode visitCoalesceExpression(CoalesceExpression node, CompilerContext context)
        {
            List<TypedByteCodeNode> operands = new ArrayList<>();
            for (Expression expression : node.getOperands()) {
                operands.add(process(expression, context));
            }

            Class<?> type = getType(operands);

            TypedByteCodeNode nullValue = coerceToType(context, process(new NullLiteral(), context), type);
            // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
            for (TypedByteCodeNode operand : Lists.reverse(operands)) {
                Block condition = new Block(context)
                        .append(coerceToType(context, operand, type).node)
                        .loadVariable("wasNull");

                // if value was null, pop the null value, clear the null flag, and process the next operand
                Block nullBlock = new Block(context)
                        .pop(type)
                        .loadConstant(false)
                        .storeVariable("wasNull")
                        .append(nullValue.node);

                nullValue = typedByteCodeNode(new IfStatement(context, condition, nullBlock, NOP), type);
            }

            return typedByteCodeNode(nullValue.node, type);
        }

        @Override
        protected TypedByteCodeNode visitInPredicate(InPredicate node, CompilerContext context)
        {
            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                throw new UnsupportedOperationException("Compilation of IN subquery is not supported yet");
            }

            TypedByteCodeNode value = process(node.getValue(), context);
            if (value.type == void.class) {
                return value;
            }

            ImmutableList.Builder<TypedByteCodeNode> values = ImmutableList.builder();
            InListExpression valueList = (InListExpression) valueListExpression;
            for (Expression test : valueList.getValues()) {
                TypedByteCodeNode testNode = process(test, context);
                values.add(testNode);
            }

            Class<?> type = getType(ImmutableList.<TypedByteCodeNode>builder()
                    .add(value)
                    .addAll(values.build()).build());

            ImmutableListMultimap.Builder<Integer, TypedByteCodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
            ImmutableList.Builder<TypedByteCodeNode> defaultBucket = ImmutableList.builder();
            ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();
            for (TypedByteCodeNode testNode : values.build()) {
                if (testNode.node instanceof Constant) {
                    Constant constant = (Constant) testNode.node;
                    Object testValue = constant.getValue();
                    constantValuesBuilder.add(testValue);
                    int hashCode;
                    if (type == boolean.class) {
                        // boolean constant is actually an integer type
                        hashCode = Operations.hashCode(((Number) testValue).intValue() != 0);
                    }
                    else if (type == long.class) {
                        hashCode = Operations.hashCode((long) testValue);
                    }
                    else if (type == double.class) {
                        hashCode = Operations.hashCode(((Number) testValue).doubleValue());
                    }
                    else if (type == Slice.class) {
                        hashCode = Operations.hashCode((Slice) testValue);
                    } else {
                        // SQL nulls are not currently encoded as constants, if they are one day, this code will need to be modified
                        throw new IllegalStateException("Error processing in statement: unsupported type " + testValue.getClass().getSimpleName());
                    }

                    hashBucketsBuilder.put(hashCode, coerceToType(context, testNode, type));
                }
                else {
                    defaultBucket.add(coerceToType(context, testNode, type));
                }
            }
            ImmutableListMultimap<Integer, TypedByteCodeNode> hashBuckets = hashBucketsBuilder.build();
            ImmutableSet<Object> constantValues = constantValuesBuilder.build();

            LabelNode end = new LabelNode("end");
            LabelNode match = new LabelNode("match");
            LabelNode noMatch = new LabelNode("noMatch");

            LabelNode defaultLabel = new LabelNode("default");

            ByteCodeNode switchBlock;
            if (constantValues.size() < 1000) {
                Block switchCaseBlocks = new Block(context);
                LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
                for (Entry<Integer, Collection<TypedByteCodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                    LabelNode label = new LabelNode("inHash" + bucket.getKey());
                    switchBuilder.addCase(bucket.getKey(), label);
                    Collection<TypedByteCodeNode> testValues = bucket.getValue();

                    Block caseBlock = buildInCase(context, type, label, match, defaultLabel, testValues, false);
                    switchCaseBlocks
                            .append(caseBlock.setDescription("case " + bucket.getKey()));
                }
                switchBuilder.defaultCase(defaultLabel);

                switchBlock = new Block(context)
                        .comment("lookupSwitch(hashCode(<stackValue>))")
                        .dup(type)
                        .invokeStatic(Operations.class, "hashCode", int.class, type)
                        .append(switchBuilder.build())
                        .append(switchCaseBlocks);
            }
            else {
                // for huge IN lists, use a Set
                FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(
                        "in",
                        getSessionByteCode,
                        ImmutableList.<TypedByteCodeNode>of(),
                        new InFunctionBinder(type, constantValues));

                switchBlock = new Block(context)
                        .comment("inListSet.contains(<stackValue>)")
                        .append(new IfStatement(context,
                                new Block(context).dup(type).invokeDynamic(functionBinding.getName(), functionBinding.getCallSite().type(), functionBinding.getBindingId()),
                                jump(match),
                                NOP));
            }

            Block defaultCaseBlock = buildInCase(context, type, defaultLabel, match, noMatch, defaultBucket.build(), true).setDescription("default");

            Block block = new Block(context)
                    .comment(node.toString())
                    .append(coerceToType(context, value, type).node)
                    .append(ifWasNullPopAndGoto(context, end, boolean.class, type))
                    .append(switchBlock)
                    .append(defaultCaseBlock);

            Block matchBlock = new Block(context)
                    .setDescription("match")
                    .visitLabel(match)
                    .pop(type)
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .loadConstant(true)
                    .gotoLabel(end);
            block.append(matchBlock);

            Block noMatchBlock = new Block(context)
                    .setDescription("noMatch")
                    .visitLabel(noMatch)
                    .pop(type)
                    .loadConstant(false)
                    .gotoLabel(end);
            block.append(noMatchBlock);

            block.visitLabel(end);

            return typedByteCodeNode(block, boolean.class);
        }

        private Block buildInCase(CompilerContext context,
                Class<?> type,
                LabelNode caseLabel,
                LabelNode matchLabel,
                LabelNode noMatchLabel,
                Collection<TypedByteCodeNode> testValues,
                boolean checkForNulls)
        {
            Variable caseWasNull = null;
            if (checkForNulls) {
                caseWasNull = context.createTempVariable(boolean.class);
            }

            Block caseBlock = new Block(context)
                    .visitLabel(caseLabel);

            if (checkForNulls) {
                caseBlock.loadConstant(false)
                        .storeVariable(caseWasNull.getLocalVariableDefinition());
            }

            LabelNode elseLabel = new LabelNode("else");
            Block elseBlock = new Block(context)
                    .visitLabel(elseLabel);

            if (checkForNulls) {
                elseBlock.loadVariable(caseWasNull.getLocalVariableDefinition())
                        .storeVariable("wasNull");
            }

            elseBlock.gotoLabel(noMatchLabel);

            ByteCodeNode elseNode = elseBlock;
            for (TypedByteCodeNode testNode : testValues) {
                LabelNode testLabel = new LabelNode("test");
                IfStatementBuilder test = ifStatementBuilder(context);

                Block condition = new Block(context)
                        .visitLabel(testLabel)
                        .dup(type)
                        .append(coerceToType(context, testNode, type).node);

                if (checkForNulls) {
                    condition.loadVariable("wasNull")
                            .storeVariable(caseWasNull.getLocalVariableDefinition())
                            .append(ifWasNullPopAndGoto(context, elseLabel, void.class, type, type));
                }
                condition.invokeStatic(Operations.class, "equal", boolean.class, type, type);
                test.condition(condition);

                test.ifTrue(new Block(context).gotoLabel(matchLabel));
                test.ifFalse(elseNode);

                elseNode = test.build();
                elseLabel = testLabel;
            }
            caseBlock.append(elseNode);
            return caseBlock;
        }

        @Override
        protected TypedByteCodeNode visitExpression(Expression node, CompilerContext context)
        {
            throw new UnsupportedOperationException(format("Compilation of %s not supported yet", node.getClass().getSimpleName()));
        }

        private ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
        }

        private ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
        }

        private ByteCodeNode ifWasNullClearPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
        }

        private ByteCodeNode ifWasNullClearPopAndGoto(CompilerContext context,
                LabelNode label,
                Class<?> returnType,
                Iterable<? extends Class<?>> stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), true);
        }

        private ByteCodeNode handleNullValue(CompilerContext context,
                LabelNode label,
                Class<?> returnType,
                List<? extends Class<?>> stackArgsToPop,
                boolean clearNullFlag)
        {
            Block nullCheck = new Block(context)
                    .setDescription("ifWasNullGoto")
                    .loadVariable("wasNull");

            String clearComment = null;
            if (clearNullFlag) {
                nullCheck.loadConstant(false).storeVariable("wasNull");
                clearComment = "clear wasNull";
            }

            Block isNull = new Block(context);
            for (Class<?> parameterType : stackArgsToPop) {
                isNull.pop(parameterType);
            }

            isNull.loadJavaDefault(returnType);
            String loadDefaultComment = null;
            if (returnType != void.class) {
                loadDefaultComment = format("loadJavaDefault(%s)", returnType.getName());
            }

            isNull.gotoLabel(label);

            String popComment = null;
            if (!stackArgsToPop.isEmpty()) {
                popComment = format("pop(%s)", Joiner.on(", ").join(stackArgsToPop));
            }

            String comment = format("if wasNull then %s", Joiner.on(", ").skipNulls().join(clearComment, popComment, loadDefaultComment, "goto " + label.getLabel()));
            return new IfStatement(context, comment, nullCheck, isNull, NOP);
        }

        private TypedByteCodeNode coerceToType(CompilerContext context, TypedByteCodeNode node, Class<?> type)
        {
            if (node.type == void.class) {
                return typedByteCodeNode(new Block(context).append(node.node).loadJavaDefault(type), type);
            }
            if (node.type == long.class && type == double.class) {
                return typedByteCodeNode(new Block(context).append(node.node).append(L2D), type);
            }
            return node;
        }

        private Class<?> getType(TypedByteCodeNode... nodes)
        {
            return getType(ImmutableList.copyOf(nodes));
        }

        private Class<?> getType(Iterable<TypedByteCodeNode> nodes)
        {
            Set<Class<?>> types = IterableTransformer.on(nodes)
                    .transform(nodeTypeGetter())
                    .select(not(Predicates.<Class<?>>equalTo(void.class)))
                    .set();

            if (types.isEmpty()) {
                return void.class;
            }
            if (types.equals(ImmutableSet.of(double.class, long.class))) {
                return double.class;
            }
            checkState(types.size() == 1, "Expected only one type but found %s", types);
            return Iterables.getOnlyElement(types);
        }

        private static Function<TypedWhenClause, TypedByteCodeNode> whenConditionGetter()
        {
            return new Function<TypedWhenClause, TypedByteCodeNode>()
            {
                @Override
                public TypedByteCodeNode apply(TypedWhenClause when)
                {
                    return when.condition;
                }
            };
        }

        private static Function<TypedWhenClause, TypedByteCodeNode> whenValueGetter()
        {
            return new Function<TypedWhenClause, TypedByteCodeNode>()
            {
                @Override
                public TypedByteCodeNode apply(TypedWhenClause when)
                {
                    return when.value;
                }
            };
        }

        public static class InFunctionBinder
                implements FunctionBinder
        {
            private static final MethodHandle inMethod;

            static {
                try {
                    inMethod = lookup().findStatic(InFunctionBinder.class, "in", MethodType.methodType(boolean.class, ImmutableSet.class, Object.class));
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

            private final Class<?> valueType;
            private final ImmutableSet<Object> constantValues;

            public InFunctionBinder(Class<?> valueType, ImmutableSet<Object> constantValues)
            {
                this.valueType = valueType;
                this.constantValues = constantValues;
            }

            @Override
            public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
            {
                MethodHandle methodHandle = inMethod.bindTo(constantValues);
                methodHandle = methodHandle.asType(MethodType.methodType(boolean.class, valueType));
                return new FunctionBinding(bindingId, name, new ConstantCallSite(methodHandle), arguments, false);
            }

            public static boolean in(ImmutableSet<?> set, Object value)
            {
                return set.contains(value);
            }
        }

        private class TypedWhenClause
        {
            private final TypedByteCodeNode condition;
            private final TypedByteCodeNode value;

            private TypedWhenClause(CompilerContext context, WhenClause whenClause)
            {
                this.condition = process(whenClause.getOperand(), context);
                this.value = process(whenClause.getResult(), context);
            }
        }
    }

    private static boolean isNumber(Class<?> type)
    {
        return type == long.class || type == double.class;
    }

    private static Function<TypedByteCodeNode, Class<?>> nodeTypeGetter()
    {
        return new Function<TypedByteCodeNode, Class<?>>()
        {
            @Override
            public Class<?> apply(TypedByteCodeNode node)
            {
                return node.type;
            }
        };
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
