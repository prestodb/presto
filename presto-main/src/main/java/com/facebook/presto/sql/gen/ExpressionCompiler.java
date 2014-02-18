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
import com.facebook.presto.byteCode.control.ForLoop;
import com.facebook.presto.byteCode.control.ForLoop.ForLoopBuilder;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.AbstractFilterAndProjectOperator;
import com.facebook.presto.operator.AbstractScanFilterAndProjectOperator;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InputReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.TraceClassVisitor;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.IdentityHashMap;
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
import static com.google.common.base.Preconditions.checkState;
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
    private final Metadata metadata;

    private final LoadingCache<OperatorCacheKey, FilterAndProjectOperatorFactoryFactory> operatorFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<OperatorCacheKey, FilterAndProjectOperatorFactoryFactory>()
            {
                @Override
                public FilterAndProjectOperatorFactoryFactory load(OperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileFilterAndProjectOperator(key.getFilter(), key.getProjections(), key.getExpressionTypes(), key.getTimeZoneKey());
                }
            });

    private final LoadingCache<OperatorCacheKey, ScanFilterAndProjectOperatorFactoryFactory> sourceOperatorFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<OperatorCacheKey, ScanFilterAndProjectOperatorFactoryFactory>()
            {
                @Override
                public ScanFilterAndProjectOperatorFactoryFactory load(OperatorCacheKey key)
                        throws Exception
                {
                    return internalCompileScanFilterAndProjectOperator(key.getSourceId(), key.getFilter(), key.getProjections(), key.getExpressionTypes(), key.getTimeZoneKey());
                }
            });

    private final AtomicLong generatedClasses = new AtomicLong();

    @Inject
    public ExpressionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
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

            Class<?> bootstrapClass = defineClass(classDefinition, Object.class, new DynamicClassLoader());

            AtomicReference<BootstrapFunctionBinder> bootstrapReference = (AtomicReference<BootstrapFunctionBinder>) bootstrapClass.getField("BOOTSTRAP").get(null);
            bootstrapReference.set(bootstrapFunctionBinder);

            bootstrapMethod = bootstrapClass.getMethod("bootstrap", Lookup.class, String.class, MethodType.class, long.class);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    @Managed
    public long getGeneratedClasses()
    {
        return generatedClasses.get();
    }

    @Managed
    public long getCachedFilterAndProjectOperators()
    {
        return operatorFactories.size();
    }

    @Managed
    public long getCachedScanFilterAndProjectOperators()
    {
        return sourceOperatorFactories.size();
    }

    public OperatorFactory compileFilterAndProjectOperator(int operatorId,
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            TimeZoneKey timeZoneKey)
    {
        return operatorFactories.getUnchecked(new OperatorCacheKey(filter, projections, expressionTypes, null, timeZoneKey)).create(operatorId);
    }

    private DynamicClassLoader createClassLoader()
    {
        return new DynamicClassLoader(bootstrapMethod.getDeclaringClass().getClassLoader());
    }

    @VisibleForTesting
    public FilterAndProjectOperatorFactoryFactory internalCompileFilterAndProjectOperator(
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            TimeZoneKey timeZoneKey)
    {
        DynamicClassLoader classLoader = createClassLoader();

        // create filter and project page iterator class
        TypedOperatorClass typedOperatorClass = compileFilterAndProjectOperator(filter, projections, expressionTypes, classLoader, timeZoneKey);

        Constructor<? extends Operator> constructor;
        try {
            constructor = typedOperatorClass.getOperatorClass().getConstructor(OperatorContext.class, Iterable.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
        FilterAndProjectOperatorFactoryFactory operatorFactoryFactory = new FilterAndProjectOperatorFactoryFactory(constructor, typedOperatorClass.getTypes());

        return operatorFactoryFactory;
    }

    private TypedOperatorClass compileFilterAndProjectOperator(
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            DynamicClassLoader classLoader,
            TimeZoneKey timeZoneKey)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterAndProjectOperator_" + CLASS_ID.incrementAndGet()),
                type(AbstractFilterAndProjectOperator.class));

        // declare fields
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("operatorContext", OperatorContext.class),
                arg("types", type(Iterable.class, Type.class)))
                .getBody()
                .comment("super(operatorContext, types);")
                .pushThis()
                .getVariable("operatorContext")
                .getVariable("types")
                .invokeConstructor(AbstractFilterAndProjectOperator.class, OperatorContext.class, Iterable.class)
                .comment("this.session = operatorContext.getSession();")
                .pushThis()
                .getVariable("operatorContext")
                .invokeVirtual(OperatorContext.class, "getSession", Session.class)
                .putField(sessionField)
                .ret();

        generateFilterAndProjectRowOriented(classDefinition, projections, expressionTypes);

        //
        // filter method
        //
        generateFilterMethod(classDefinition, filter, expressionTypes, true, timeZoneKey);
        generateFilterMethod(classDefinition, filter, expressionTypes, false, timeZoneKey);

        //
        // project methods
        //
        List<Type> types = new ArrayList<>();
        int projectionIndex = 0;
        for (Expression projection : projections) {
            generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, expressionTypes, true, timeZoneKey);
            generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, expressionTypes, false, timeZoneKey);
            types.add(expressionTypes.get(projection));
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

        Class<? extends Operator> filterAndProjectClass = defineClass(classDefinition, Operator.class, classLoader);
        return new TypedOperatorClass(filterAndProjectClass, types);
    }

    public SourceOperatorFactory compileScanFilterAndProjectOperator(
            int operatorId,
            PlanNodeId sourceId,
            DataStreamProvider dataStreamProvider,
            List<ColumnHandle> columns,
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            TimeZoneKey timeZoneKey)
    {
        OperatorCacheKey cacheKey = new OperatorCacheKey(filter, projections, expressionTypes, sourceId, timeZoneKey);
        return sourceOperatorFactories.getUnchecked(cacheKey).create(operatorId, dataStreamProvider, columns);
    }

    @VisibleForTesting
    public ScanFilterAndProjectOperatorFactoryFactory internalCompileScanFilterAndProjectOperator(
            PlanNodeId sourceId,
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            TimeZoneKey timeZoneKey)
    {
        DynamicClassLoader classLoader = createClassLoader();

        // create filter and project page iterator class
        TypedOperatorClass typedOperatorClass = compileScanFilterAndProjectOperator(filter, projections, expressionTypes, classLoader, timeZoneKey);

        Constructor<? extends SourceOperator> constructor;
        try {
            constructor = typedOperatorClass.getOperatorClass().asSubclass(SourceOperator.class).getConstructor(
                    OperatorContext.class,
                    PlanNodeId.class,
                    DataStreamProvider.class,
                    Iterable.class,
                    Iterable.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        ScanFilterAndProjectOperatorFactoryFactory operatorFactoryFactory = new ScanFilterAndProjectOperatorFactoryFactory(
                constructor,
                sourceId,
                typedOperatorClass.getTypes());

        return operatorFactoryFactory;
    }

    private TypedOperatorClass compileScanFilterAndProjectOperator(
            Expression filter,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes,
            DynamicClassLoader classLoader,
            TimeZoneKey timeZoneKey)
    {
        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(bootstrapMethod),
                a(PUBLIC, FINAL),
                typeFromPathName("ScanFilterAndProjectOperator_" + CLASS_ID.incrementAndGet()),
                type(AbstractScanFilterAndProjectOperator.class));

        // declare fields
        FieldDefinition sessionField = classDefinition.declareField(a(PRIVATE, FINAL), "session", Session.class);

        // constructor
        classDefinition.declareConstructor(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                arg("operatorContext", OperatorContext.class),
                arg("sourceId", PlanNodeId.class),
                arg("dataStreamProvider", DataStreamProvider.class),
                arg("columns", type(Iterable.class, ColumnHandle.class)),
                arg("types", type(Iterable.class, Type.class)))
                .getBody()
                .comment("super(operatorContext, sourceId, dataStreamProvider, columns, types);")
                .pushThis()
                .getVariable("operatorContext")
                .getVariable("sourceId")
                .getVariable("dataStreamProvider")
                .getVariable("columns")
                .getVariable("types")
                .invokeConstructor(AbstractScanFilterAndProjectOperator.class, OperatorContext.class, PlanNodeId.class, DataStreamProvider.class, Iterable.class, Iterable.class)
                .comment("this.session = operatorContext.getSession();")
                .pushThis()
                .getVariable("operatorContext")
                .invokeVirtual(OperatorContext.class, "getSession", Session.class)
                .putField(sessionField)
                .ret();

        generateFilterAndProjectRowOriented(classDefinition, projections, expressionTypes);
        generateFilterAndProjectCursorMethod(classDefinition, projections);

        //
        // filter method
        //
        generateFilterMethod(classDefinition, filter, expressionTypes, true, timeZoneKey);
        generateFilterMethod(classDefinition, filter, expressionTypes, false, timeZoneKey);

        //
        // project methods
        //
        List<Type> types = new ArrayList<>();
        int projectionIndex = 0;
        for (Expression projection : projections) {
            generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, expressionTypes, true, timeZoneKey);
            generateProjectMethod(classDefinition, "project_" + projectionIndex, projection, expressionTypes, false, timeZoneKey);
            types.add(expressionTypes.get(projection));
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

        Class<? extends SourceOperator> filterAndProjectClass = defineClass(classDefinition, SourceOperator.class, classLoader);
        return new TypedOperatorClass(filterAndProjectClass, types);
    }

    private void generateFilterAndProjectRowOriented(ClassDefinition classDefinition,
            List<Expression> projections,
            IdentityHashMap<Expression, Type> expressionTypes)
    {
        MethodDefinition filterAndProjectMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filterAndProjectRowOriented",
                type(void.class),
                arg("page", com.facebook.presto.operator.Page.class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = filterAndProjectMethod.getCompilerContext();

        LocalVariableDefinition positionVariable = compilerContext.declareVariable(int.class, "position");

        LocalVariableDefinition rowsVariable = compilerContext.declareVariable(int.class, "rows");
        filterAndProjectMethod.getBody()
                .comment("int rows = page.getPositionCount();")
                .getVariable("page")
                .invokeVirtual(com.facebook.presto.operator.Page.class, "getPositionCount", int.class)
                .putVariable(rowsVariable);

        List<LocalVariableDefinition> cursorVariables = new ArrayList<>();
        int channels = getMaxInputChannel(expressionTypes) + 1;
        for (int i = 0; i < channels; i++) {
            LocalVariableDefinition cursorVariable = compilerContext.declareVariable(BlockCursor.class, "cursor_" + i);
            cursorVariables.add(cursorVariable);
            filterAndProjectMethod.getBody()
                    .comment("BlockCursor %s = page.getBlock(%s).cursor();", cursorVariable.getName(), i)
                    .getVariable("page")
                    .push(i)
                    .invokeVirtual(com.facebook.presto.operator.Page.class, "getBlock", com.facebook.presto.spi.block.Block.class, int.class)
                    .invokeInterface(com.facebook.presto.spi.block.Block.class, "cursor", BlockCursor.class)
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
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
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
        condition.invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), nCopies(channels, type(BlockCursor.class)));
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
                        ImmutableList.<ParameterizedType>builder().addAll(nCopies(channels, type(BlockCursor.class))).add(type(BlockBuilder.class)).build());
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
                    .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(Preconditions.class, "checkState", void.class, boolean.class);
        }

        filterAndProjectMethod.getBody().ret();
    }

    private void generateFilterAndProjectCursorMethod(ClassDefinition classDefinition, List<Expression> projections)
    {
        MethodDefinition filterAndProjectMethod = classDefinition.declareMethod(new CompilerContext(bootstrapMethod),
                a(PUBLIC),
                "filterAndProjectRowOriented",
                type(int.class),
                arg("cursor", RecordCursor.class),
                arg("pageBuilder", PageBuilder.class));

        CompilerContext compilerContext = filterAndProjectMethod.getCompilerContext();

        LocalVariableDefinition completedPositionsVariable = compilerContext.declareVariable(int.class, "completedPositions");
        filterAndProjectMethod.getBody()
                .comment("int completedPositions = 0;")
                .putVariable(completedPositionsVariable, 0);

        //
        // for loop loop body
        //
        LabelNode done = new LabelNode("done");
        ForLoopBuilder forLoop = ForLoop.forLoopBuilder(compilerContext)
                .initialize(NOP)
                .condition(new Block(compilerContext)
                        .comment("completedPositions < 16384")
                        .getVariable(completedPositionsVariable)
                        .push(16384)
                        .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class)
                )
                .update(new Block(compilerContext)
                        .comment("completedPositions++")
                        .incrementVariable(completedPositionsVariable, (byte) 1)
                );

        Block forLoopBody = new Block(compilerContext);
        forLoop.body(forLoopBody);

        forLoopBody.comment("if (pageBuilder.isFull()) break;")
                .append(new Block(compilerContext)
                        .getVariable("pageBuilder")
                        .invokeVirtual(PageBuilder.class, "isFull", boolean.class)
                        .ifTrueGoto(done));

        forLoopBody.comment("if (!cursor.advanceNextPosition()) break;")
                .append(new Block(compilerContext)
                        .getVariable("cursor")
                        .invokeInterface(RecordCursor.class, "advanceNextPosition", boolean.class)
                        .ifFalseGoto(done));

        // if (filter(cursor))
        IfStatementBuilder ifStatement = new IfStatementBuilder(compilerContext);
        ifStatement.condition(new Block(compilerContext)
                .pushThis()
                .getVariable("cursor")
                .invokeVirtual(classDefinition.getType(), "filter", type(boolean.class), type(RecordCursor.class)));

        Block trueBlock = new Block(compilerContext);
        ifStatement.ifTrue(trueBlock);
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
        forLoopBody.append(ifStatement.build());

        filterAndProjectMethod.getBody()
                .append(forLoop.build())
                .visitLabel(done)
                .comment("return completedPositions;")
                .getVariable("completedPositions")
                .retInt();
    }

    private void generateFilterMethod(ClassDefinition classDefinition,
            Expression filter,
            IdentityHashMap<Expression, Type> expressionTypes,
            boolean sourceIsCursor,
            TimeZoneKey timeZoneKey)
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
                    toBlockCursorParameters(expressionTypes));
        }

        filterMethod.comment("Filter: %s", filter.toString());

        filterMethod.getCompilerContext().declareVariable(type(boolean.class), "wasNull");
        Block getSessionByteCode = new Block(filterMethod.getCompilerContext()).pushThis().getField(classDefinition.getType(), "session", type(Session.class));
        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(metadata, bootstrapFunctionBinder, expressionTypes, getSessionByteCode, sourceIsCursor, timeZoneKey);
        ByteCodeNode body = visitor.process(filter, filterMethod.getCompilerContext());

        LabelNode end = new LabelNode("end");
        filterMethod
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable("wasNull", false)
                .append(body)
                .getVariable("wasNull")
                .ifFalseGoto(end)
                .pop(boolean.class)
                .push(false)
                .visitLabel(end)
                .retBoolean();
    }

    private Class<?> generateProjectMethod(ClassDefinition classDefinition,
            String methodName,
            Expression projection,
            IdentityHashMap<Expression, Type> expressionTypes,
            boolean sourceIsCursor,
            TimeZoneKey timeZoneKey)
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
            parameters.addAll(toBlockCursorParameters(expressionTypes));
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
        ByteCodeExpressionVisitor visitor = new ByteCodeExpressionVisitor(metadata, bootstrapFunctionBinder, expressionTypes, getSessionByteCode, sourceIsCursor, timeZoneKey);
        ByteCodeNode body = visitor.process(projection, context);

        projectionMethod
                .getBody()
                .comment("boolean wasNull = false;")
                .putVariable("wasNull", false)
                .getVariable("output")
                .append(body);

        Type projectionType = expressionTypes.get(projection);
        Block notNullBlock = new Block(context);
        if (projectionType.getJavaType() == boolean.class) {
            notNullBlock
                    .comment("output.append(<booleanStackValue>);")
                    .invokeInterface(BlockBuilder.class, "append", BlockBuilder.class, boolean.class)
                    .pop();
        }
        else if (projectionType.getJavaType() == long.class) {
            notNullBlock
                    .comment("output.append(<longStackValue>);")
                    .invokeInterface(BlockBuilder.class, "append", BlockBuilder.class, long.class)
                    .pop();
        }
        else if (projectionType.getJavaType() == double.class) {
            notNullBlock
                    .comment("output.append(<doubleStackValue>);")
                    .invokeInterface(BlockBuilder.class, "append", BlockBuilder.class, double.class)
                    .pop();
        }
        else if (projectionType.getJavaType() == Slice.class) {
            notNullBlock
                    .comment("output.append(<sliceStackValue>);")
                    .invokeInterface(BlockBuilder.class, "append", BlockBuilder.class, Slice.class)
                    .pop();
        }
        else {
            throw new UnsupportedOperationException("Type " + projectionType + " can not be output yet");
        }

        Block nullBlock = new Block(context)
                .comment("output.appendNull();")
                .pop(projectionType.getJavaType())
                .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                .pop();

        projectionMethod.getBody()
                .comment("if the result was null, appendNull; otherwise append the value")
                .append(new IfStatement(context, new Block(context).getVariable("wasNull"), nullBlock, notNullBlock))
                .ret();

        return projectionType.getJavaType();
    }

    private Integer getMaxInputChannel(IdentityHashMap<Expression, Type> expressionTypes)
    {
        int maxInputChannel = -1;
        for (Expression expression : expressionTypes.keySet()) {
            if (expression instanceof InputReference) {
                maxInputChannel = Math.max(maxInputChannel, ((InputReference) expression).getInput().getChannel());
            }
        }
        return maxInputChannel;
    }

    private static class TypedOperatorClass
    {
        private final Class<? extends Operator> operatorClass;
        private final List<Type> types;

        private TypedOperatorClass(Class<? extends Operator> operatorClass, List<Type> types)
        {
            this.operatorClass = operatorClass;
            this.types = types;
        }

        private Class<? extends Operator> getOperatorClass()
        {
            return operatorClass;
        }

        private List<Type> getTypes()
        {
            return types;
        }
    }

    private List<NamedParameterDefinition> toBlockCursorParameters(IdentityHashMap<Expression, Type> expressionTypes)
    {
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        int channels = getMaxInputChannel(expressionTypes) + 1;
        for (int i = 0; i < channels; i++) {
            parameters.add(arg("channel_" + i, BlockCursor.class));
        }
        return parameters.build();
    }

    private <T> Class<? extends T> defineClass(ClassDefinition classDefinition, Class<T> superType, DynamicClassLoader classLoader)
    {
        Class<?> clazz = defineClasses(ImmutableList.of(classDefinition), classLoader).values().iterator().next();
        return clazz.asSubclass(superType);
    }

    private Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions, DynamicClassLoader classLoader)
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
            if (RUN_ASM_VERIFIER) {
                ClassReader reader = new ClassReader(byteCode);
                CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
            }
            byteCodes.put(classDefinition.getType().getJavaClassName(), byteCode);
        }

        String dumpClassPath = DUMP_CLASS_FILES_TO.get();
        if (dumpClassPath != null) {
            for (Entry<String, byte[]> entry : byteCodes.entrySet()) {
                File file = new File(dumpClassPath, ParameterizedType.typeFromJavaClassName(entry.getKey()).getClassName() + ".class");
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
        Map<String, Class<?>> classes = classLoader.defineClasses(byteCodes);
        generatedClasses.addAndGet(classes.size());
        return classes;
    }

    private static final class OperatorCacheKey
    {
        private final Expression filter;
        private final List<Expression> projections;
        private final IdentityHashMap<Expression, Type> expressionTypes;
        private final PlanNodeId sourceId;
        private final TimeZoneKey timeZoneKey;

        private OperatorCacheKey(Expression expression, List<Expression> projections, IdentityHashMap<Expression, Type> expressionTypes, PlanNodeId sourceId, TimeZoneKey timeZoneKey)
        {
            this.filter = expression;
            this.projections = ImmutableList.copyOf(projections);
            this.expressionTypes = expressionTypes;
            this.sourceId = sourceId;
            this.timeZoneKey = timeZoneKey;
        }

        private Expression getFilter()
        {
            return filter;
        }

        private List<Expression> getProjections()
        {
            return projections;
        }

        private IdentityHashMap<Expression, Type> getExpressionTypes()
        {
            return expressionTypes;
        }

        private PlanNodeId getSourceId()
        {
            return sourceId;
        }

        public TimeZoneKey getTimeZoneKey()
        {
            return timeZoneKey;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(filter, projections, expressionTypes, sourceId, timeZoneKey);
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
            OperatorCacheKey other = (OperatorCacheKey) obj;
            return Objects.equal(this.filter, other.filter) &&
                    Objects.equal(this.projections, other.projections) &&
                    Objects.equal(this.expressionTypes, other.expressionTypes) &&
                    Objects.equal(this.sourceId, other.sourceId) &&
                    Objects.equal(this.timeZoneKey, other.timeZoneKey);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filter", filter)
                    .add("projections", projections)
                    .add("expressionTypes", expressionTypes)
                    .add("sourceId", sourceId)
                    .add("timeZoneKey", timeZoneKey)
                    .toString();
        }
    }

    private static class FilterAndProjectOperatorFactoryFactory
    {
        private final Constructor<? extends Operator> constructor;
        private final List<Type> types;

        public FilterAndProjectOperatorFactoryFactory(Constructor<? extends Operator> constructor, List<Type> types)
        {
            this.constructor = checkNotNull(constructor, "constructor is null");
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        public OperatorFactory create(int operatorId)
        {
            return new FilterAndProjectOperatorFactory(constructor, operatorId, types);
        }
    }

    private static class FilterAndProjectOperatorFactory
            implements OperatorFactory
    {
        private final Constructor<? extends Operator> constructor;
        private final int operatorId;
        private final List<Type> types;
        private boolean closed;

        public FilterAndProjectOperatorFactory(
                Constructor<? extends Operator> constructor,
                int operatorId,
                List<Type> types)
        {
            this.constructor = checkNotNull(constructor, "constructor is null");
            this.operatorId = operatorId;
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, constructor.getDeclaringClass().getSimpleName());
            try {
                return constructor.newInstance(operatorContext, types);
            }
            catch (InvocationTargetException e) {
                throw Throwables.propagate(e.getCause());
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class ScanFilterAndProjectOperatorFactoryFactory
    {
        private final Constructor<? extends SourceOperator> constructor;
        private final PlanNodeId sourceId;
        private final List<Type> types;

        public ScanFilterAndProjectOperatorFactoryFactory(
                Constructor<? extends SourceOperator> constructor,
                PlanNodeId sourceId,
                List<Type> types)
        {
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.constructor = checkNotNull(constructor, "constructor is null");
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        public SourceOperatorFactory create(int operatorId, DataStreamProvider dataStreamProvider, List<ColumnHandle> columns)
        {
            return new ScanFilterAndProjectOperatorFactory(constructor, operatorId, sourceId, dataStreamProvider, columns, types);
        }
    }

    private static class ScanFilterAndProjectOperatorFactory
            implements SourceOperatorFactory
    {
        private final Constructor<? extends SourceOperator> constructor;
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final DataStreamProvider dataStreamProvider;
        private final List<ColumnHandle> columns;
        private final List<Type> types;
        private boolean closed;

        public ScanFilterAndProjectOperatorFactory(
                Constructor<? extends SourceOperator> constructor,
                int operatorId,
                PlanNodeId sourceId,
                DataStreamProvider dataStreamProvider,
                List<ColumnHandle> columns,
                List<Type> types)
        {
            this.constructor = checkNotNull(constructor, "constructor is null");
            this.operatorId = operatorId;
            this.sourceId = checkNotNull(sourceId, "sourceId is null");
            this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
            this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, constructor.getDeclaringClass().getSimpleName());
            try {
                return constructor.newInstance(operatorContext, sourceId, dataStreamProvider, columns, types);
            }
            catch (InvocationTargetException e) {
                throw Throwables.propagate(e.getCause());
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
