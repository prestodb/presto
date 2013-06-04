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
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.TraceClassVisitor;

import javax.inject.Inject;
import java.io.PrintWriter;
import java.lang.invoke.CallSite;
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
import static com.facebook.presto.byteCode.OpCodes.I2L;
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
import static com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode.typedByteCodeNode;
import static com.facebook.presto.sql.gen.SliceConstant.sliceConstant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.nCopies;

public class ExpressionCompiler
{
    private static final AtomicLong CLASS_ID = new AtomicLong();

    private final Method bootstrapMethod;
    private final BootstrapFunctionBinder bootstrapFunctionBinder;

    private final LoadingCache<OperatorCacheKey, Function<Operator, Operator>> operatorFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<OperatorCacheKey, Function<Operator, Operator>>()
            {
                @Override
                public Function<Operator, Operator> load(OperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileFilterAndProjectOperator(key.getFilter(), key.getProjections(), key.getInputTypes());
                }
            });

    private final LoadingCache<ExpressionCacheKey, FilterFunction> filters = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<ExpressionCacheKey, FilterFunction>()
    {
        @Override
        public FilterFunction load(ExpressionCacheKey key)
                throws Exception
        {
            return internalCompileFilterFunction(key.getExpression(), key.getInputTypes());
        }
    });

    private final LoadingCache<ExpressionCacheKey, ProjectionFunction> projections = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<ExpressionCacheKey, ProjectionFunction>()
    {
        @Override
        public ProjectionFunction load(ExpressionCacheKey key)
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

    public Function<Operator, Operator> compileFilterAndProjectOperator(Expression filter, List<Expression> projections, Map<Input, Type> inputTypes)
    {
        return operatorFactories.getUnchecked(new OperatorCacheKey(filter, projections, inputTypes));
    }

    public FilterFunction compileFilterFunction(Expression expression, ImmutableMap<Input, Type> inputTypes)
    {
        return filters.getUnchecked(new ExpressionCacheKey(expression, inputTypes));
    }

    public ProjectionFunction compileProjectionFunction(Expression expression, ImmutableMap<Input, Type> inputTypes)
    {
        return projections.getUnchecked(new ExpressionCacheKey(expression, inputTypes));
    }

    @VisibleForTesting
    public Function<Operator, Operator> internalCompileFilterAndProjectOperator(Expression filter, List<Expression> projections, Map<Input, Type> inputTypes)
    {
        DynamicClassLoader classLoader = createClassLoader();

        // create filter and project page iterator class
        TypedPageIteratorClass typedPageIteratorClass = compileFilterAndProjectIterator(filter, projections, inputTypes, classLoader);

        // create and operator for the class
        Class<? extends Operator> operatorClass = compileOperatorClass(typedPageIteratorClass.getPageIteratorClass(), classLoader);

        // create an factory for the operator
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

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("tupleInfos", type(Iterable.class, TupleInfo.class)),
                arg("pageIterator", PageIterator.class))
                .getBody()
                .loadThis()
                .loadVariable("tupleInfos")
                .loadVariable("pageIterator")
                .invokeConstructor(AbstractFilterAndProjectIterator.class, Iterable.class, PageIterator.class)
                .ret();

        generateFilterAndProjectMethod(classDefinition, projections, inputTypes);

        //
        // filter method
        //
        generateFilterMethod(classDefinition, filter, inputTypes);

        //
        // project methods
        //
        List<TupleInfo> tupleInfos = new ArrayList<>();
        int projectionIndex = 0;
        for (Expression projection : projections) {
            Class<?> type = generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, inputTypes);
            // todo remove assumption that void and boolean is a long
            if (type == long.class || type == void.class || type == boolean.class) {
                tupleInfos.add(TupleInfo.SINGLE_LONG);
            }
            else if (type == double.class) {
                tupleInfos.add(TupleInfo.SINGLE_DOUBLE);
            }
            else if (type == Slice.class) {
                tupleInfos.add(TupleInfo.SINGLE_VARBINARY);
            } else {
                throw new IllegalStateException("Type " + type.getName() + "can be output");
            }
            projectionIndex++;
        }

        Class<? extends PageIterator> filterAndProjectClass = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(PageIterator.class);
        return new TypedPageIteratorClass(filterAndProjectClass, tupleInfos);
    }

    private void generateFilterAndProjectMethod(ClassDefinition classDefinition,
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

        // int rows = extendedPriceBlock.getPositionCount();
        LocalVariableDefinition rowsVariable = compilerContext.declareVariable(int.class, "rows");
        filterAndProjectMethod.getBody()
                .loadVariable("blocks")
                .loadConstant(0)
                .loadObjectArray()
                .invokeInterface(com.facebook.presto.block.Block.class, "getPositionCount", int.class)
                .storeVariable(rowsVariable);


        // BlockCursor extendedPriceCursor = extendedPriceBlock.cursor();
        List<LocalVariableDefinition> cursorVariables = new ArrayList<>();
        int channels = Ordering.natural().max(transform(inputTypes.keySet(), Input.channelGetter())) + 1;
        for (int i = 0; i < channels; i++) {
            LocalVariableDefinition cursorVariable = compilerContext.declareVariable(BlockCursor.class, "cursor_" + i);
            cursorVariables.add(cursorVariable);
            filterAndProjectMethod.getBody()
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
                    .loadVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext);
        Block condition = new Block(compilerContext);
        condition.loadThis();
        for (int channel = 0; channel < channels; channel++) {
            condition.loadVariable("cursor_" + channel);
        }
        condition.invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), nCopies(channels, type(TupleReadable.class)));
        ifStatement.condition(condition);

        Block trueBlock = new Block(compilerContext);
        if (projections.isEmpty()) {
            trueBlock.loadVariable("pageBuilder").invokeVirtual(PageBuilder.class, "declarePosition", void.class);
        }
        else {
            // pageBuilder.getBlockBuilder(0).append(cursor.getDouble(0);
            for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
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
                    .loadVariable(cursorVariable)
                    .invokeInterface(BlockCursor.class, "advanceNextPosition", boolean.class)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        filterAndProjectMethod.getBody().ret();
    }

    private Function<Operator, Operator> compileOperatorFactoryClass(List<TupleInfo> tupleInfos, Class<? extends Operator> operatorClass, DynamicClassLoader classLoader)
    {
        Constructor<? extends Operator> constructor;
        try {
            constructor = operatorClass.getConstructor(List.class, Operator.class);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProjectOperatorFactory_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(Function.class, Operator.class, Operator.class));

        FieldDefinition tupleInfoField = classDefinition.declareField(a(PRIVATE, FINAL), "tupleInfo", type(List.class, TupleInfo.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC), arg("tupleInfos", type(List.class, TupleInfo.class)))
                .getBody()
                .loadThis()
                .invokeConstructor(Object.class)
                .loadThis()
                .loadVariable("tupleInfos")
                .putField(classDefinition.getType(), tupleInfoField)
                .ret();

        // apply method
        MethodDefinition applyMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "apply",
                type(Object.class),
                arg("source", Object.class));

        applyMethod.getBody()
                .newObject(operatorClass)
                .dup()
                .loadThis()
                .getField(classDefinition.getType(), tupleInfoField)
                .loadVariable("source")
                .invokeConstructor(constructor)
                .retObject();

        Class<? extends Function<Operator, Operator>> factoryClass = (Class<? extends Function<Operator, Operator>>)
                defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(Function.class);

        try {
            Constructor<? extends Function<Operator, Operator>> factoryConstructor = factoryClass.getConstructor(List.class);
            Function<Operator, Operator> factory = factoryConstructor.newInstance(tupleInfos);
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

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC), arg("tupleInfos", type(List.class, TupleInfo.class)), arg("source", Operator.class))
                .getBody()
                .loadThis()
                .loadVariable("tupleInfos")
                .loadVariable("source")
                .invokeConstructor(AbstractFilterAndProjectOperator.class, List.class, Operator.class)
                .ret();

        MethodDefinition iteratorMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "iterator",
                type(PageIterator.class),
                arg("source", PageIterator.class));

        iteratorMethod.getBody()
                .newObject(iteratorClass)
                .dup()
                .loadThis()
                .invokeInterface(Operator.class, "getTupleInfos", List.class)
                .loadVariable("source")
                .invokeConstructor(iteratorClass, Iterable.class, PageIterator.class)
                .retObject();

        return defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next().asSubclass(Operator.class);
    }

    @VisibleForTesting
    public FilterFunction internalCompileFilterFunction(Expression expression, Map<Input, Type> inputTypes)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(FilterFunction.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC))
                .getBody()
                .loadThis()
                .invokeConstructor(Object.class)
                .ret();

        // filter function
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

        // filter method with unrolled channels
        generateFilterMethod(classDefinition, expression, inputTypes);

        // define the class
        Class<? extends FilterFunction> filterClass = defineClasses(ImmutableList.of(classDefinition), createClassLoader()).values().iterator().next().asSubclass(FilterFunction.class);

        // create instance
        try {
            FilterFunction function = filterClass.newInstance();
            return function;
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private void generateFilterMethod(ClassDefinition classDefinition,
            Expression filter,
            Map<Input, Type> inputTypes)
    {
        MethodDefinition filterMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    "filter",
                    type(boolean.class),
                    toTupleReaderParameters(inputTypes));

        filterMethod.getCompilerContext().declareVariable(type(boolean.class), "wasNull");
        TypedByteCodeNode body = new Visitor(bootstrapFunctionBinder, inputTypes).process(filter, filterMethod.getCompilerContext());

        if (body.type == void.class) {
            filterMethod
                    .getBody()
                    .loadConstant(false)
                    .retBoolean();
        }
        else {
            filterMethod
                    .getBody()
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .append(body.node)
                    .retBoolean();
        }
    }

    @VisibleForTesting
    public ProjectionFunction internalCompileProjectionFunction(Expression expression, Map<Input, Type> inputTypes)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("ProjectionFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(ProjectionFunction.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod), a(PUBLIC))
                .getBody()
                .loadThis()
                .invokeConstructor(Object.class)
                .ret();

        // void project(TupleReadable[] channels, BlockBuilder output)
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

        // projection with unrolled channels
        Class<?> type = generateProjectMethod(classDefinition, "project", expression, inputTypes);


        // TupleInfo getTupleInfo();
        MethodDefinition getTupleInfoMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "getTupleInfo",
                type(TupleInfo.class));

        // todo remove assumption that void and boolean is a long
        if (type == long.class || type == void.class || type == boolean.class) {
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

        // define the class
        Class<? extends ProjectionFunction> projectionClass = defineClasses(ImmutableList.of(classDefinition), createClassLoader()).values().iterator().next().asSubclass(ProjectionFunction.class);

        // create instance
        try {
            ProjectionFunction function = projectionClass.newInstance();
            return function;
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private Class<?> generateProjectMethod(ClassDefinition classDefinition,
            String methodName,
            Expression projection,
            Map<Input, Type> inputTypes)
    {
            ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
            parameters.addAll(toTupleReaderParameters(inputTypes));
            parameters.add(arg("output", BlockBuilder.class));

        MethodDefinition projectionMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                    a(PUBLIC),
                    methodName,
                    type(void.class),
                    parameters.build());

        // generate body code
        CompilerContext context = projectionMethod.getCompilerContext();
        context.declareVariable(type(boolean.class), "wasNull");
        TypedByteCodeNode body = new Visitor(bootstrapFunctionBinder, inputTypes).process(projection, context);

        if (body.type != void.class) {
            projectionMethod
                    .getBody()
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .loadVariable("output")
                    .append(body.node);

            Block notNullBlock = new Block(context);
            if (body.type == boolean.class) {
                notNullBlock.append(I2L).invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class);
            }
            else if (body.type == long.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class);
            }
            else if (body.type == double.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, double.class);
            }
            else if (body.type == Slice.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, Slice.class);
            }
            else {
                throw new UnsupportedOperationException("Type " + body.type + " can not be output yet");
            }

            Block nullBlock = new Block(context)
                    .pop(body.type)
                    .invokeVirtual(BlockBuilder.class, "appendNull", BlockBuilder.class);

            projectionMethod.getBody()
                    .append(new IfStatement(context, new Block(context).loadVariable("wasNull"), nullBlock, notNullBlock))
                    .ret();
        }
        else {
            projectionMethod
                    .getBody()
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

        if (false) {
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
//            if (true) {
//                ClassReader reader = new ClassReader(byteCode);
//                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
//            }
            byteCodes.put(classDefinition.getType(), byteCode);
        }
//        if (classDebugPath.isPresent()) {
//            for (Entry<ParameterizedType, byte[]> entry : byteCodes.entrySet()) {
//                try {
//                    File file = new File(classDebugPath.get(), entry.getKey().getClassName() + ".class");
//                    System.err.println("ClassFile: " + file.getAbsolutePath());
//                    Files.createParentDirs(file);
//                    Files.write(entry.getValue(), file);
//                }
//                catch (IOException e) {
//                    System.err.println("failed writing file: " + e.getMessage());
//                }
//            }
//        }
        if (false) {
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

        public Visitor(BootstrapFunctionBinder bootstrapFunctionBinder, Map<Input, Type> inputTypes)
        {
            this.bootstrapFunctionBinder = bootstrapFunctionBinder;
            this.inputTypes = inputTypes;
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

            int field = input.getField();
            Block isNullCheck = new Block(context)
                    .setDescription(String.format("channel_%d.get%s(%d)", channel, type, field))
                    .loadVariable("channel_" + channel)
                    .loadConstant(field)
                    .invokeInterface(TupleReadable.class, "isNull", boolean.class, int.class);

            Block isNull = new Block(context)
                    .loadConstant(true)
                    .storeVariable("wasNull");

            Block notNull = new Block(context)
                    .loadVariable("channel_" + channel)
                    .loadConstant(field);

            Class<?> nodeType;
            switch (type) {
                case BOOLEAN:
                case LONG:
                    isNull.loadConstant(0L);
                    notNull.invokeInterface(TupleReadable.class, "getLong", long.class, int.class);
                    nodeType = long.class;
                    break;
                case DOUBLE:
                    isNull.loadConstant(0.0);
                    notNull.invokeInterface(TupleReadable.class, "getDouble", double.class, int.class);
                    nodeType = double.class;
                    break;
                case STRING:
                    isNull.loadNull();
                    notNull.invokeInterface(TupleReadable.class, "getSlice", Slice.class, int.class);
                    nodeType = Slice.class;
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }

            return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, notNull), nodeType);
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

            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(node.getName(), arguments);
            return visitFunctionBinding(context, functionBinding);
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
            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(functionName, ImmutableList.of(expression));
            return visitFunctionBinding(context, functionBinding);
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

            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction("like", arguments, new LikeFunctionBinder());
            return visitFunctionBinding(context, functionBinding);
        }

        private TypedByteCodeNode visitFunctionBinding(CompilerContext context, FunctionBinding functionBinding)
        {
            List<TypedByteCodeNode> arguments;
            arguments = functionBinding.getArguments();
            MethodType methodType = functionBinding.getCallSite().type();

            LabelNode end = new LabelNode("end");
            Block block = new Block(context);
            ArrayList<Class<?>> stackTypes = new ArrayList<>();
            for (int i = 0; i < arguments.size(); i++) {
                TypedByteCodeNode argument = arguments.get(i);
                Class<?> argumentType = methodType.parameterList().get(i);
                block.append(coerceToType(context, argument, argumentType).node);

                stackTypes.add(argument.type);
                block.append(ifWasNullPopAndGoto(context, end, methodType.returnType(), Lists.reverse(stackTypes)));
            }
            block.invokeDynamic(functionBinding.getName(), methodType, functionBinding.getBindingId());
            block.visitLabel(end);

            return typedByteCodeNode(block, methodType.returnType());
        }

        @Override
        public TypedByteCodeNode visitCast(Cast node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getExpression(), context);

            Block block = new Block(context);
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
                throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }

            Block block = new Block(context);
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
                    throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
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
                throw new UnsupportedOperationException(String.format("not yet implemented: negate(%s)", value.type));
            }

            // simple single op so there is no reason to do a null check
            Block block = new Block(context)
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
                    return visitAnd(context, left, right);

                }
                case OR: {
                    return visitOr(context, left, right);
                }
            }
            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
        }

        private TypedByteCodeNode visitAnd(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right)
        {
            Block block = new Block(context);

            block.append(left.node);

            IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                    .condition(new Block(context).loadVariable("wasNull"));

            LabelNode end = new LabelNode("end");
            ifLeftIsNull.ifTrue(new Block(context)
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .pop()
                    .loadConstant(true));

            LabelNode leftIsTrue = new LabelNode("leftIsTrue");
            ifLeftIsNull.ifFalse(new Block(context)
                    .ifNotZeroGoto(leftIsTrue)
                            // left is false, so we are done
                    .loadConstant(false)
                    .gotoLabel(end)
                    .visitLabel(leftIsTrue)
                    .append(loadBoolean(false)));

            block.append(ifLeftIsNull.build());

            // eval right!
            block.append(right.node);

            IfStatementBuilder ifRightIsNull = ifStatementBuilder(context);
            ifRightIsNull.condition(new Block(context).loadVariable("wasNull"));

            ifRightIsNull.ifTrue(new Block(context).pop());

            ifRightIsNull.ifFalse(new Block(context).append(new IfStatement(context,
                    NOP,
                    new Block(context).storeVariable("wasNull").loadConstant(true),
                    new Block(context).pop().loadConstant(false))));

            block.append(ifRightIsNull.build())
                    .visitLabel(end);

            return typedByteCodeNode(block, boolean.class);
        }

        private TypedByteCodeNode visitOr(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right)
        {
            Block block = new Block(context);

            block.append(left.node);

            IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                    .condition(new Block(context).loadVariable("wasNull"));

            LabelNode end = new LabelNode("end");
            ifLeftIsNull.ifTrue(new Block(context)
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .pop()
                    .loadConstant(true));

            LabelNode leftIsFalse = new LabelNode("leftIsFalse");
            ifLeftIsNull.ifFalse(new Block(context)
                    .ifZeroGoto(leftIsFalse)
                            // left is true, so we are done
                    .loadConstant(true)
                    .gotoLabel(end)
                    .visitLabel(leftIsFalse)
                    .append(loadBoolean(false)));

            block.append(ifLeftIsNull.build());

            // eval right!
            block.append(right.node);

            IfStatementBuilder ifRightIsNull = ifStatementBuilder(context);
            ifRightIsNull.condition(new Block(context).loadVariable("wasNull"));

            ifRightIsNull.ifTrue(new Block(context).pop());

            ifRightIsNull.ifFalse(new Block(context).append(new IfStatement(context,
                    NOP,
                    new Block(context).pop().loadConstant(true),
                    new Block(context).storeVariable("wasNull").loadConstant(false))));

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
                    .append(value.node)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitComparisonExpression(ComparisonExpression node, CompilerContext context)
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
                    throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }

            LabelNode end = new LabelNode("end");
            Block block = new Block(context);

            block.append(coerceToType(context, left, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, left.type));

            block.append(coerceToType(context, right, type).node);
            block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, right.type));

            block.invokeStatic(Operations.class, function, boolean.class, type, type);
            return typedByteCodeNode(block.visitLabel(end), boolean.class);
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
            Block block = new Block(context);

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

                // clear null flag after evaluating condition
                block.loadConstant(false)
                        .storeVariable("wasNull");

                elseValue = typedByteCodeNode(new IfStatement(context, condition, coerceToType(context, whenClause.value, resultType).node, elseValue.node), resultType);
            }

            return typedByteCodeNode(block.append(elseValue.node), resultType);
        }

        @Override
        protected TypedByteCodeNode visitNullIfExpression(NullIfExpression node, CompilerContext context)
        {
            TypedByteCodeNode first = process(node.getFirst(), context);
            TypedByteCodeNode second = process(node.getSecond(), context);
            if (first.type == void.class || second.type == void.class) {
                return first;
            }

            Class<?> type = getType(first, second);

            LabelNode notMatch = new LabelNode("notMatch");
            Block block = new Block(context)
                    .append(coerceToType(context, first, type).node)
                    .dup(type)
                    .append(ifWasNullPopAndGoto(context, notMatch, void.class, type))
                    .append(coerceToType(context, second, type).node)
                    .append(ifWasNullClearPopAndGoto(context, notMatch, void.class, type, type));

            Block conditionBlock = new Block(context)
                    .invokeStatic(Operations.class, "equal", boolean.class, type, type);

            Block trueBlock = new Block(context)
                    .loadConstant(true)
                    .storeVariable("wasNull")
                    .pop(type)
                    .loadJavaDefault(type);

            block.append(new IfStatement(context, conditionBlock, trueBlock, notMatch));

            return typedByteCodeNode(block, type);
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

            ImmutableListMultimap.Builder<Integer, TypedByteCodeNode> hashBuckets = ImmutableListMultimap.builder();
            ImmutableList.Builder<TypedByteCodeNode> defaultBucket = ImmutableList.builder();
            for (TypedByteCodeNode testNode : values.build()) {
                if (testNode.node instanceof Constant) {
                    Constant constant = (Constant) testNode.node;
                    Object testValue = constant.getValue();
                    int hashCode;
                    if (type == boolean.class) {
                        // boolean constant is actually and integer type
                        hashCode = Operations.hashCode(testValue != 0);
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

                    hashBuckets.put(hashCode, coerceToType(context, testNode, type));
                }
                else {
                    defaultBucket.add(coerceToType(context, testNode, type));
                }
            }

            LabelNode end = new LabelNode("end");
            LabelNode match = new LabelNode("match");
            LabelNode noMatch = new LabelNode("noMatch");

            LabelNode defaultLabel = new LabelNode("default");

            Block switchCaseBlocks = new Block(context);
            LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
            for (Entry<Integer, Collection<TypedByteCodeNode>> bucket : hashBuckets.build().asMap().entrySet()) {
                LabelNode label = new LabelNode("inHash" + bucket.getKey());
                switchBuilder.addCase(bucket.getKey(), label);
                Collection<TypedByteCodeNode> testValues = bucket.getValue();

                Block caseBlock = buildCase(context, type, label, match, defaultLabel, testValues, false);
                switchCaseBlocks.append(caseBlock.setDescription("case " + bucket.getKey()));
            }

            switchBuilder.defaultCase(defaultLabel);
            Block caseBlock = buildCase(context, type, defaultLabel, match, noMatch, defaultBucket.build(), true);
            switchCaseBlocks.append(caseBlock.setDescription("default"));

            Block block = new Block(context)
                    .append(coerceToType(context, value, type).node)
                    .append(ifWasNullPopAndGoto(context, end, boolean.class, type))
                    .dup(type)
                    .invokeStatic(Operations.class, "hashCode", int.class, type)
                    .append(switchBuilder.build())
                    .append(switchCaseBlocks);

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

        private Block buildCase(CompilerContext context,
                Class<?> type,
                LabelNode caseLabel,
                LabelNode matchLabel,
                LabelNode noMatchLabel,
                Collection<TypedByteCodeNode> testValues, boolean checkForNulls)
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
            throw new UnsupportedOperationException(String.format("Compilation of %s not supported yet", node.getClass().getSimpleName()));
        }

        private ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, ImmutableList.copyOf(stackArgsToPop), false);
        }

        private ByteCodeNode ifWasNullPopAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Iterable<? extends Class<?>> stackArgsToPop)
        {
            return handleNullValue(context, label, returnType, stackArgsToPop, false);
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
            return handleNullValue(context, label, returnType, stackArgsToPop, true);
        }

        private ByteCodeNode handleNullValue(CompilerContext context,
                LabelNode label,
                Class<?> returnType,
                Iterable<? extends Class<?>> stackArgsToPop,
                boolean clearNullFlag)
        {
            Block nullCheck = new Block(context)
                    .setDescription("ifWasNullGoto")
                    .loadVariable("wasNull");

            if (clearNullFlag) {
                nullCheck.loadConstant(false).storeVariable("wasNull");
            }

            Block isNull = new Block(context);
            for (Class<?> parameterType : stackArgsToPop) {
                isNull.pop(parameterType);
            }
            isNull.loadJavaDefault(returnType);
            isNull.gotoLabel(label);

            return new IfStatement(context, nullCheck, isNull, NOP);
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
            Set<Class<?>> types = ImmutableSet.copyOf(filter(transform(nodes, nodeTypeGetter()), not(Predicates.<Class<?>>equalTo(void.class))));
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
            return Objects.toStringHelper(this)
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
            return Objects.toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("inputTypes", inputTypes)
                    .toString();
        }
    }
}
