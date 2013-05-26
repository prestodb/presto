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
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
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
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.objectweb.asm.ClassWriter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.OpCodes.L2D;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.ParameterizedType.typeFromPathName;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode.typedByteCodeNode;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
        TypedByteCodeNode body = new Visitor(session, inputTypes).process(expression, filterMethod.getCompilerContext());

        filterMethod
                .getBody()
                .append(body.node)
                .retBoolean();

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
        TypedByteCodeNode body = new Visitor(session, inputTypes).process(expression, projectionMethod.getCompilerContext());

        projectionMethod
                .getBody()
                .loadVariable("output")
                .append(body.node);

        if (body.type == long.class) {
            projectionMethod.getBody()
                    .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, long.class)
                    .ret();
        }
        else if (body.type == double.class) {
            projectionMethod.getBody()
                    .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, double.class)
                    .ret();
        }
        else if (body.type == String.class) {
            projectionMethod.getBody()
                    .invokeVirtual(BlockBuilder.class, "append", BlockBuilder.class, String.class)
                    .ret();
        }

        // TupleInfo getTupleInfo();
        MethodDefinition getTupleInfoMethod = classDefinition.declareMethod(new CompilerContext(),
                a(PUBLIC),
                "getTupleInfo",
                type(TupleInfo.class));

        if (body.type == long.class) {
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

        DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(System.out);
        for (ClassDefinition classDefinition : classDefinitions) {
            dumpByteCode.visitClass(classDefinition);
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
//        if (DUMP_BYTE_CODE) {
//            verifyClasses(byteCodes, classLoader, true, new PrintWriter(System.err));
//            for (byte[] byteCode : byteCodes.values()) {
//                ClassReader classReader = new ClassReader(byteCode);
//                classReader.accept(new TraceClassVisitor(new PrintWriter(System.err)), ClassReader.SKIP_FRAMES);
//            }
//        }
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
        public TypedByteCodeNode visitInputReference(InputReference node, CompilerContext context)
        {
            Input input = node.getInput();
            int channel = input.getChannel();
            Type type = inputTypes.get(input);
            checkState(type != null, "No type for input %s", input);

            int field = 0;
            Block block = new Block(context)
                    .setDescription(String.format("channels[%d].get%s(%d)", channel, type, field))
                    .loadVariable("channels")
                    .loadConstant(channel)
                    .loadObjectArray()
                    .loadConstant(field);


            Class<?> nodeType;
            switch (type) {
                case FIXED_INT_64:
                    block.invokeInterface(TupleReadable.class, "getLong", long.class, int.class);
                    nodeType = long.class;
                    break;
                case DOUBLE:
                    block.invokeInterface(TupleReadable.class, "getDouble", double.class, int.class);
                    nodeType = double.class;
                    break;
                case VARIABLE_BINARY:
                    block.invokeInterface(TupleReadable.class, "getSlice", Slice.class, int.class);
                    block.invokeStatic(Operations.class, "toString", String.class, Slice.class);
                    nodeType = String.class;
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }

            return typedByteCodeNode(block, nodeType);
        }

        @Override
        public TypedByteCodeNode visitCast(Cast node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getExpression(), context);

            Block block = new Block(context);
            block.append(value.node);

            switch (node.getType()) {
                case "BOOLEAN":
                    block.invokeStatic(Operations.class, "castToBoolean", boolean.class, value.type);
                    return typedByteCodeNode(block, boolean.class);
                case "BIGINT":
                    block.invokeStatic(Operations.class, "castToLong", long.class, value.type);
                    return typedByteCodeNode(block, long.class);
                case "DOUBLE":
                    block.invokeStatic(Operations.class, "castToDouble", double.class, value.type);
                    return typedByteCodeNode(block, double.class);
                case "VARCHAR":
                    block.invokeStatic(Operations.class, "castToString", String.class, value.type);
                    return typedByteCodeNode(block, String.class);
            }
            throw new UnsupportedOperationException("Unsupported type: " + node.getType());

        }

        @Override
        protected TypedByteCodeNode visitArithmeticExpression(ArithmeticExpression node, CompilerContext context)
        {
            TypedByteCodeNode left = process(node.getLeft(), context);
            TypedByteCodeNode right = process(node.getRight(), context);

            Block block = new Block(context);

            if (isNumber(left.type) && isNumber(right.type)) {
                if (left.type == long.class && right.type == long.class) {
                    block.append(left.node);
                    block.append(right.node);
                    switch (node.getType()) {
                        case ADD:
                            block.invokeStatic(Operations.class, "add", long.class, long.class, long.class);
                            break;
                        case SUBTRACT:
                            block.invokeStatic(Operations.class, "subtract", long.class, long.class, long.class);
                            break;
                        case MULTIPLY:
                            block.invokeStatic(Operations.class, "multiply", long.class, long.class, long.class);
                            break;
                        case DIVIDE:
                            block.invokeStatic(Operations.class, "divide", long.class, long.class, long.class);
                            break;
                        case MODULUS:
                            block.invokeStatic(Operations.class, "modulus", long.class, long.class, long.class);
                            break;
                        default:
                            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                    }
                    return typedByteCodeNode(block, long.class);
                }
                else {
                    block.append(left.node);
                    if (left.type == long.class) {
                        block.append(L2D);
                    }
                    block.append(right.node);
                    if (right.type == long.class) {
                        block.append(L2D);
                    }

                    switch (node.getType()) {
                        case ADD:
                            block.invokeStatic(Operations.class, "add", double.class, double.class, double.class);
                            break;
                        case SUBTRACT:
                            block.invokeStatic(Operations.class, "subtract", double.class, double.class, double.class);
                            break;
                        case MULTIPLY:
                            block.invokeStatic(Operations.class, "multiply", double.class, double.class, double.class);
                            break;
                        case DIVIDE:
                            block.invokeStatic(Operations.class, "divide", double.class, double.class, double.class);
                            break;
                        case MODULUS:
                            block.invokeStatic(Operations.class, "modulus", double.class, double.class, double.class);
                            break;
                        default:
                            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                    }
                    return typedByteCodeNode(block, double.class);
                }
            }
            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
        }

        @Override
        protected TypedByteCodeNode visitNegativeExpression(NegativeExpression node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);


            if (isNumber(value.type)) {
                if (value.type == long.class) {
                    return typedByteCodeNode(new Block(context)
                            .append(value.node)
                            .invokeStatic(Operations.class, "negate", long.class, long.class),
                            long.class);
                }
                else {
                    return typedByteCodeNode(new Block(context)
                            .append(value.node)
                            .invokeStatic(Operations.class, "negate", double.class, double.class),
                            double.class);
                }
            }
            throw new UnsupportedOperationException(String.format("not yet implemented: negate(%s)", value.type));
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
            TypedByteCodeNode left = process(node.getValue(), context);
            Preconditions.checkState(left.type == boolean.class);
            return typedByteCodeNode(new Block(context)
                    .append(left.node)
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class), boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitComparisonExpression(ComparisonExpression node, CompilerContext context)
        {
            TypedByteCodeNode left = process(node.getLeft(), context);
            TypedByteCodeNode right = process(node.getRight(), context);

            Block block = new Block(context);

            if (isNumber(left.type) && isNumber(right.type)) {
                if (left.type == long.class && right.type == long.class) {
                    block.append(left.node);
                    block.append(right.node);
                    switch (node.getType()) {
                        case EQUAL:
                            block.invokeStatic(Operations.class, "equal", boolean.class, long.class, long.class);
                            break;
                        case NOT_EQUAL:
                            block.invokeStatic(Operations.class, "notEqual", boolean.class, long.class, long.class);
                            break;
                        case LESS_THAN:
                            block.invokeStatic(Operations.class, "lessThan", boolean.class, long.class, long.class);
                            break;
                        case LESS_THAN_OR_EQUAL:
                            block.invokeStatic(Operations.class, "lessThanOrEqual", boolean.class, long.class, long.class);
                            break;
                        case GREATER_THAN:
                            block.invokeStatic(Operations.class, "greaterThan", boolean.class, long.class, long.class);
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            block.invokeStatic(Operations.class, "greaterThanOrEqual", boolean.class, long.class, long.class);
                            break;
                        default:
                            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                    }
                }
                else {
                    block.append(left.node);
                    if (left.type == long.class) {
                        block.append(L2D);
                    }
                    block.append(right.node);
                    if (right.type == long.class) {
                        block.append(L2D);
                    }

                    switch (node.getType()) {
                        case EQUAL:
                            block.invokeStatic(Operations.class, "equal", boolean.class, double.class, double.class);
                            break;
                        case NOT_EQUAL:
                            block.invokeStatic(Operations.class, "notEqual", boolean.class, double.class, double.class);
                            break;
                        case LESS_THAN:
                            block.invokeStatic(Operations.class, "lessThan", boolean.class, double.class, double.class);
                            break;
                        case LESS_THAN_OR_EQUAL:
                            block.invokeStatic(Operations.class, "lessThanOrEqual", boolean.class, double.class, double.class);
                            break;
                        case GREATER_THAN:
                            block.invokeStatic(Operations.class, "greaterThan", boolean.class, double.class, double.class);
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            block.invokeStatic(Operations.class, "greaterThanOrEqual", boolean.class, double.class, double.class);
                            break;
                        default:
                            throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                    }
                }
            }
            else if (left.type == String.class && right.type == String.class) {
                block.append(left.node);
                block.append(right.node);
                switch (node.getType()) {
                    case EQUAL:
                        block.invokeStatic(Operations.class, "equal", boolean.class, String.class, String.class);
                        break;
                    case NOT_EQUAL:
                        block.invokeStatic(Operations.class, "notEqual", boolean.class, String.class, String.class);
                        break;
                    case LESS_THAN:
                        block.invokeStatic(Operations.class, "lessThan", boolean.class, String.class, String.class);
                        break;
                    case LESS_THAN_OR_EQUAL:
                        block.invokeStatic(Operations.class, "lessThanOrEqual", boolean.class, String.class, String.class);
                        break;
                    case GREATER_THAN:
                        block.invokeStatic(Operations.class, "greaterThan", boolean.class, String.class, String.class);
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        block.invokeStatic(Operations.class, "greaterThanOrEqual", boolean.class, String.class, String.class);
                        break;
                    default:
                        throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                }
            }
            else if (left.type == boolean.class && right.type == boolean.class) {
                block.append(left.node);
                block.append(right.node);
                switch (node.getType()) {
                    case EQUAL:
                        block.invokeStatic(Operations.class, "equal", boolean.class, boolean.class, boolean.class);
                        break;
                    case NOT_EQUAL:
                        block.invokeStatic(Operations.class, "notEqual", boolean.class, boolean.class, boolean.class);
                        break;
                    default:
                        throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
                }
            }
            else {
                throw new UnsupportedOperationException(String.format("not yet implemented: %s(%s, %s)", node.getType(), left.type, right.type));
            }
            return typedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitBetweenPredicate(BetweenPredicate node, CompilerContext context)
        {
            TypedByteCodeNode value = process(node.getValue(), context);
            TypedByteCodeNode min = process(node.getMin(), context);
            TypedByteCodeNode max = process(node.getMax(), context);

            Block block = new Block(context);
            if (value.type == long.class) {
                block.append(value.node)
                        .append(min.node)
                        .append(max.node)
                        .invokeStatic(Operations.class, "between", boolean.class, long.class, long.class, long.class);
            }
            else if (value.type == double.class) {
                block.append(value.node);

                block.append(min.node);
                if (min.type == long.class) {
                    block.append(L2D);
                }

                block.append(max.node);
                if (max.type == long.class) {
                    block.append(L2D);
                }
                block.invokeStatic(Operations.class, "between", boolean.class, double.class, double.class, double.class);
            }
            else if (value.type == String.class) {
                block.append(value.node)
                        .append(min.node)
                        .append(max.node)
                        .invokeStatic(Operations.class, "between", boolean.class, String.class, String.class, String.class);
            }
            else {
                throw new UnsupportedOperationException(String.format("Between not supported for type %s", value.type));
            }
            return typedByteCodeNode(block, boolean.class);
        }

        @Override
        protected TypedByteCodeNode visitIfExpression(IfExpression node, CompilerContext context)
        {
            TypedByteCodeNode condition = process(node.getCondition(), context);
            Preconditions.checkState(condition.type == boolean.class);
            TypedByteCodeNode trueValue = process(node.getTrueValue(), context);
            TypedByteCodeNode falseValue;
            if (node.getFalseValue().isPresent()) {
                falseValue = process(node.getFalseValue().get(), context);
            } else {
                throw new UnsupportedOperationException(String.format("If with no else is not supported yet"));
            }
            return typedByteCodeNode(new IfStatement(context, condition.node, trueValue.node, falseValue.node), trueValue.type);
        }

        @Override
        protected TypedByteCodeNode visitSearchedCaseExpression(SearchedCaseExpression node, CompilerContext context)
        {
            Expression defaultValue = node.getDefaultValue();
            if (defaultValue == null) {
                throw new UnsupportedOperationException(String.format("Case with no default is not supported yet"));
            }

            TypedByteCodeNode elseValue = process(defaultValue, context);
            for (WhenClause whenClause : Lists.reverse(new ArrayList<>(node.getWhenClauses()))) {
                TypedByteCodeNode condition = process(whenClause.getOperand(), context);
                Preconditions.checkState(condition.type == boolean.class);
                TypedByteCodeNode trueValue = process(whenClause.getResult(), context);

                elseValue = typedByteCodeNode(new IfStatement(context, condition.node, trueValue.node, elseValue.node), trueValue.type);
            }

            return elseValue;
        }

        @Override
        protected TypedByteCodeNode visitExpression(Expression node, CompilerContext context)
        {
            throw new UnsupportedOperationException(String.format("Compilation of %s not supported yet", node.getClass().getSimpleName()));
        }

        private boolean isNumber(Class<?> type)
        {
            return type == long.class || type == double.class;
        }
    }
}
