/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.gen;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.OpCodes.L2D;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode.typedByteCodeNode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class ExpressionCompiler
{
    private static final AtomicLong CLASS_ID = new AtomicLong();
    private final Session session;
    private final Map<Symbol, Input> layout;
    private final ImmutableMap<Input, Type> inputTypes;

    public ExpressionCompiler(Session session, Map<Symbol, Input> layout, final List<TupleInfo> tupleInfos)
    {
        this.session = checkNotNull(session, "session is null");
        this.layout = ImmutableMap.copyOf(checkNotNull(layout, "layout is null"));

        Builder<Input, Type> inputTypes = ImmutableMap.builder();
        for (Input input : layout.values()) {
            inputTypes.put(input, tupleInfos.get(input.getChannel()).getTypes().get(input.getField()));
        }
        this.inputTypes = inputTypes.build();
    }

    public FilterFunction compileFilterFunction(Expression expression)
    {
        expression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(layout), expression);

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(),
                a(PUBLIC, FINAL),
                typeFromPathName("FilterFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(FilterFunction.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(), a(PUBLIC))
                .getBody()
                .loadThis()
                .invokeConstructor(Object.class)
                .ret();

        // filter function
        MethodDefinition filterMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "filter",
                type(boolean.class),
                NamedParameterDefinition.arg("channels", TupleReadable[].class));

        // generate body code
        filterMethod.getCompilerContext().declareVariable(type(boolean.class), "wasNull");
        TypedByteCodeNode body = new Visitor(session, inputTypes).process(expression, filterMethod.getCompilerContext());

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

        // define the class
        Class<? extends FilterFunction> filterClass = defineClasses(ImmutableList.of(classDefinition)).values().iterator().next().asSubclass(FilterFunction.class);

        // create instance
        try {
            FilterFunction function = filterClass.newInstance();
            return function;
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    public ProjectionFunction compileProjectionFunction(Expression expression)
    {
        expression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(layout), expression);

        ClassDefinition classDefinition = new ClassDefinition(new CompilerContext(),
                a(PUBLIC, FINAL),
                typeFromPathName("ProjectionFunction_" + CLASS_ID.incrementAndGet()),
                type(Object.class),
                type(ProjectionFunction.class));

        // constructor
        classDefinition.declareConstructor(new CompilerContext(), a(PUBLIC))
                .getBody()
                .loadThis()
                .invokeConstructor(Object.class)
                .ret();

        // void project(TupleReadable[] channels, BlockBuilder output)
        MethodDefinition projectionMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "project",
                type(void.class),
                NamedParameterDefinition.arg("channels", TupleReadable[].class),
                NamedParameterDefinition.arg("output", BlockBuilder.class));

        // generate body code
        CompilerContext context = projectionMethod.getCompilerContext();
        context.declareVariable(type(boolean.class), "wasNull");
        TypedByteCodeNode body = new Visitor(session, inputTypes).process(expression, context);

        if (body.type != void.class) {
            projectionMethod
                    .getBody()
                    .loadConstant(false)
                    .storeVariable("wasNull")
                    .loadVariable("output")
                    .append(body.node);

            Block notNullBlock = new Block(context);
            if (body.type == long.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class);
            }
            else if (body.type == double.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, double.class);
            }
            else if (body.type == String.class) {
                notNullBlock.invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, String.class);
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

        // TupleInfo getTupleInfo();
        MethodDefinition getTupleInfoMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "getTupleInfo",
                type(TupleInfo.class));

        // todo remove assumption that void is a long
        if (body.type == long.class || body.type == void.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_LONG", type(TupleInfo.class))
                    .retObject();
        }
        else if (body.type == double.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_DOUBLE", type(TupleInfo.class))
                    .retObject();
        }
        else if (body.type == String.class) {
            getTupleInfoMethod.getBody()
                    .getStaticField(type(TupleInfo.class), "SINGLE_VARBINARY", type(TupleInfo.class))
                    .retObject();
        }


        // define the class
        Class<? extends ProjectionFunction> projectionClass = defineClasses(ImmutableList.of(classDefinition)).values().iterator().next().asSubclass(ProjectionFunction.class);

        // create instance
        try {
            ProjectionFunction function = projectionClass.newInstance();
            return function;
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    private static final DynamicClassLoader classLoader = new DynamicClassLoader();

    private static Map<String, Class<?>> defineClasses(List<ClassDefinition> classDefinitions)
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
//            verifyClasses(byteCodes, classLoader, true, new PrintWriter(System.err));
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
    }

    private static class Visitor
            extends AstVisitor<TypedByteCodeNode, CompilerContext>
    {
        private final Session session;
        private final Map<Input, Type> inputTypes;

        public Visitor(Session session, Map<Input, Type> inputTypes)
        {
            this.session = session;
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
            return typedByteCodeNode(loadString(node.getValue()), String.class);
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

            int field = 0;
            Block isNullCheck = new Block(context)
                    .setDescription(String.format("channels[%d].get%s(%d)", channel, type, field))
                    .loadVariable("channels")
                    .loadConstant(channel)
                    .loadObjectArray()
                    .loadConstant(field)
                    .invokeInterface(TupleReadable.class, "isNull", boolean.class, int.class);

            Block isNull = new Block(context)
                    .loadConstant(true)
                    .storeVariable("wasNull");

            Block notNull = new Block(context)
                    .loadVariable("channels")
                    .loadConstant(channel)
                    .loadObjectArray()
                    .loadConstant(field);

            Class<?> nodeType;
            switch (type) {
                case FIXED_INT_64:
                    isNull.loadConstant(0L);
                    notNull.invokeInterface(TupleReadable.class, "getLong", long.class, int.class);
                    nodeType = long.class;
                    break;
                case DOUBLE:
                    isNull.loadConstant(0.0);
                    notNull.invokeInterface(TupleReadable.class, "getDouble", double.class, int.class);
                    nodeType = double.class;
                    break;
                case VARIABLE_BINARY:
                    isNull.loadNull();
                    notNull.invokeInterface(TupleReadable.class, "getSlice", Slice.class, int.class);
                    notNull.invokeStatic(Operations.class, "toString", String.class, Slice.class);
                    nodeType = String.class;
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }

            return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, notNull), nodeType);
        }

        @Override
        public TypedByteCodeNode visitCast(Cast node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getExpression(), context);
            if (value.type == void.class) {
                return value;
            }

            LabelNode end = new LabelNode("end");
            Block block = new Block(context);
            block.append(value.node);

            switch (node.getType()) {
                case "BOOLEAN":
                    block.append(ifWasNullClearAndGoto(context, end, boolean.class, value.type));
                    block.invokeStatic(Operations.class, "castToBoolean", boolean.class, value.type);
                    return typedByteCodeNode(block.visitLabel(end), boolean.class);
                case "BIGINT":
                    block.append(ifWasNullClearAndGoto(context, end, long.class, value.type));
                    block.invokeStatic(Operations.class, "castToLong", long.class, value.type);
                    return typedByteCodeNode(block.visitLabel(end), long.class);
                case "DOUBLE":
                    block.append(ifWasNullClearAndGoto(context, end, double.class, value.type));
                    block.invokeStatic(Operations.class, "castToDouble", double.class, value.type);
                    return typedByteCodeNode(block.visitLabel(end), double.class);
                case "VARCHAR":
                    block.append(ifWasNullClearAndGoto(context, end, String.class, value.type));
                    block.invokeStatic(Operations.class, "castToString", String.class, value.type);
                    return typedByteCodeNode(block.visitLabel(end), String.class);
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
            block.append(ifWasNullClearAndGoto(context, end, type, left.type));

            block.append(coerceToType(context, right, type).node);
            block.append(ifWasNullClearAndGoto(context, end, type, type, right.type));

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
            Preconditions.checkState(left.type == boolean.class);
            TypedByteCodeNode right = process(node.getRight(), context);
            Preconditions.checkState(right.type == boolean.class);
            Block block = new Block(context)
                    .append(left.node)
                    .append(right.node);

            switch (node.getType()) {
                case AND: {
                    block.invokeStatic(Operations.class, "and", boolean.class, boolean.class, boolean.class);
                    return typedByteCodeNode(block, boolean.class);
                }
                case OR: {
                    block.invokeStatic(Operations.class, "or", boolean.class, boolean.class, boolean.class);
                    return typedByteCodeNode(block, boolean.class);
                }
            }
            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
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
            block.append(ifWasNullClearAndGoto(context, end, boolean.class, left.type));

            block.append(coerceToType(context, right, type).node);
            block.append(ifWasNullClearAndGoto(context, end, boolean.class, type, right.type));

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
            block.append(ifWasNullClearAndGoto(context, end, boolean.class, type));

            block.append(coerceToType(context, min, type).node);
            block.append(ifWasNullClearAndGoto(context, end, boolean.class, type, type));

            block.append(coerceToType(context, max, type).node);
            block.append(ifWasNullClearAndGoto(context, end, boolean.class, type, type, type));

            block.invokeStatic(Operations.class, "between", boolean.class, type, type, type);
            return typedByteCodeNode(block.visitLabel(end), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitIfExpression(IfExpression node, CompilerContext context)
        {
            TypedByteCodeNode conditionValue = process(node.getCondition(), context);
            Preconditions.checkState(conditionValue.type == boolean.class);

            // clear null flag after evaluating condition
            Block condition = new Block(context)
                    .append(conditionValue.node)
                    .loadConstant(false)
                    .storeVariable("wasNull");

            TypedByteCodeNode trueValue = process(node.getTrueValue(), context);
            TypedByteCodeNode falseValue = process(node.getFalseValue().or(new NullLiteral()), context);

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
            } else {
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
            TypedByteCodeNode value = process(node.getOperand(), context);
            if (value.type == void.class) {
                return value;
            }
            Variable tempVariable = context.createTempVariable(value.type);

            Block block = new Block(context)
                    .append(value.node)
                    .storeVariable(tempVariable.getLocalVariableDefinition());

            TypedByteCodeNode elseValue;
            if (node.getDefaultValue() != null) {
                elseValue = process(node.getDefaultValue(), context);
            } else {
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
                Block condition = new Block(context);
                condition.loadVariable(tempVariable.getLocalVariableDefinition());

                condition.append(coerceToType(context, whenClause.condition, getType(value, whenClause.condition)).node);
                condition.invokeStatic(Operations.class, "equal", boolean.class, value.type, value.type);

                // clear null flag after evaluating condition
                block.loadConstant(false)
                        .storeVariable("wasNull");

                elseValue = typedByteCodeNode(new IfStatement(context, condition, coerceToType(context, whenClause.value, type).node, elseValue.node), type);
            }

            return typedByteCodeNode(block.append(elseValue.node), type);
        }

        @Override
        protected TypedByteCodeNode visitExpression(Expression node, CompilerContext context)
        {
            throw new UnsupportedOperationException(String.format("Compilation of %s not supported yet", node.getClass().getSimpleName()));
        }

        private ByteCodeNode ifWasNullClearAndGoto(CompilerContext context, LabelNode label, Class<?> returnType, Class<?>... stackArgsToPop)
        {
            Block nullCheck = new Block(context)
                    .setDescription("ifWasNullGoto")
                    .loadVariable("wasNull");

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

        private boolean isNumber(Class<?> type)
        {
            return type == long.class || type == double.class;
        }

        private Function<TypedByteCodeNode, Class<?>> nodeTypeGetter()
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

        private Function<TypedWhenClause, TypedByteCodeNode> whenValueGetter()
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
}
