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
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.control.LookupSwitch.LookupSwitchBuilder;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.spi.RecordCursor;
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
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.byteCode.OpCodes.L2D;
import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.control.IfStatement.ifStatementBuilder;
import static com.facebook.presto.byteCode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.SliceConstant.sliceConstant;
import static com.facebook.presto.sql.gen.TypedByteCodeNode.typedByteCodeNode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class ByteCodeExpressionVisitor
        extends AstVisitor<TypedByteCodeNode, CompilerContext>
{
    private final BootstrapFunctionBinder bootstrapFunctionBinder;
    private final Map<Input, Type> inputTypes;
    private final ByteCodeNode getSessionByteCode;
    private final boolean sourceIsCursor;

    public ByteCodeExpressionVisitor(BootstrapFunctionBinder bootstrapFunctionBinder, Map<Input, Type> inputTypes, ByteCodeNode getSessionByteCode, boolean sourceIsCursor)
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
        return typedByteCodeNode(new Block(context).putVariable("wasNull", true), void.class);
    }

    @Override
    public TypedByteCodeNode visitInputReference(InputReference node, CompilerContext context)
    {
        Input input = node.getInput();
        int channel = input.getChannel();
        Type type = inputTypes.get(input);
        checkState(type != null, "No type for input %s", input);

        if (sourceIsCursor) {
            Block isNullCheck = new Block(context)
                    .setDescription(format("cursor.get%s(%d)", type, channel))
                    .getVariable("cursor")
                    .push(channel)
                    .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

            switch (type) {
                case BOOLEAN: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(boolean.class);

                    Block isNotNull = new Block(context)
                            .getVariable("cursor")
                            .push(channel)
                            .invokeInterface(RecordCursor.class, "getBoolean", boolean.class, int.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), boolean.class);
                }
                case BIGINT: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(long.class);

                    Block isNotNull = new Block(context)
                            .getVariable("cursor")
                            .push(channel)
                            .invokeInterface(RecordCursor.class, "getLong", long.class, int.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), long.class);
                }
                case DOUBLE: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(double.class);

                    Block isNotNull = new Block(context)
                            .getVariable("cursor")
                            .push(channel)
                            .invokeInterface(RecordCursor.class, "getDouble", double.class, int.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), double.class);
                }
                case VARCHAR: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(Slice.class);

                    Block isNotNull = new Block(context)
                            .getVariable("cursor")
                            .push(channel)
                            .invokeInterface(RecordCursor.class, "getString", byte[].class, int.class)
                            .invokeStatic(Slices.class, "wrappedBuffer", Slice.class, byte[].class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), Slice.class);
                }
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
        else {
            Block isNullCheck = new Block(context)
                    .setDescription(format("channel_%d.get%s()", channel, type))
                    .getVariable("channel_" + channel)
                    .invokeInterface(TupleReadable.class, "isNull", boolean.class);

            switch (type) {
                case BOOLEAN: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(boolean.class);

                    Block isNotNull = new Block(context)
                            .getVariable("channel_" + channel)
                            .invokeInterface(TupleReadable.class, "getBoolean", boolean.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), boolean.class);
                }
                case BIGINT: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(long.class);

                    Block isNotNull = new Block(context)
                            .getVariable("channel_" + channel)
                            .invokeInterface(TupleReadable.class, "getLong", long.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), long.class);
                }
                case DOUBLE: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(double.class);

                    Block isNotNull = new Block(context)
                            .getVariable("channel_" + channel)
                            .invokeInterface(TupleReadable.class, "getDouble", double.class);

                    return typedByteCodeNode(new IfStatement(context, isNullCheck, isNull, isNotNull), double.class);
                }
                case VARCHAR: {
                    Block isNull = new Block(context)
                            .putVariable("wasNull", true)
                            .pushJavaDefault(Slice.class);

                    Block isNotNull = new Block(context)
                            .getVariable("channel_" + channel)
                            .invokeInterface(TupleReadable.class, "getSlice", Slice.class);

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
            if (typedByteCodeNode.getType() == void.class) {
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
        if (expression.getType() == void.class) {
            return expression;
        }

        if (node.getField() == Extract.Field.TIMEZONE_HOUR || node.getField() == Extract.Field.TIMEZONE_MINUTE) {
            // TODO: we assume all times are UTC for now
            return typedByteCodeNode(new Block(context).append(expression.getNode()).pop(long.class).push(0L), long.class);
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
            if (typedByteCodeNode.getType() == void.class) {
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
            block.append(coerceToType(context, argument, argumentType).getNode());

            stackTypes.add(argument.getType());
            block.append(ifWasNullPopAndGoto(context, end, unboxedReturnType, Lists.reverse(stackTypes)));
        }
        block.invokeDynamic(functionBinding.getName(), methodType, functionBinding.getBindingId());

        if (functionBinding.isNullable()) {
            if (unboxedReturnType.isPrimitive()) {
                LabelNode notNull = new LabelNode("notNull");
                block.dup(methodType.returnType())
                        .ifNotNullGoto(notNull)
                        .putVariable("wasNull", true)
                        .comment("swap boxed null with unboxed default")
                        .pop(methodType.returnType())
                        .pushJavaDefault(unboxedReturnType)
                        .gotoLabel(end)
                        .visitLabel(notNull)
                        .append(unboxPrimitive(context, unboxedReturnType));
            }
            else {
                block.dup(methodType.returnType())
                        .ifNotNullGoto(end)
                        .putVariable("wasNull", true);
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
        block.append(value.getNode());

        if (value.getType() == void.class) {
            switch (node.getType()) {
                case "BOOLEAN":
                    block.pushJavaDefault(boolean.class);
                    return typedByteCodeNode(block, boolean.class);
                case "BIGINT":
                    block.pushJavaDefault(long.class);
                    return typedByteCodeNode(block, long.class);
                case "DOUBLE":
                    block.pushJavaDefault(double.class);
                    return typedByteCodeNode(block, double.class);
                case "VARCHAR":
                    block.pushJavaDefault(Slice.class);
                    return typedByteCodeNode(block, Slice.class);
            }
        }
        else {
            LabelNode end = new LabelNode("end");
            switch (node.getType()) {
                case "BOOLEAN":
                    block.append(ifWasNullPopAndGoto(context, end, boolean.class, value.getType()));
                    block.invokeStatic(Operations.class, "castToBoolean", boolean.class, value.getType());
                    return typedByteCodeNode(block.visitLabel(end), boolean.class);
                case "BIGINT":
                    block.append(ifWasNullPopAndGoto(context, end, long.class, value.getType()));
                    block.invokeStatic(Operations.class, "castToLong", long.class, value.getType());
                    return typedByteCodeNode(block.visitLabel(end), long.class);
                case "DOUBLE":
                    block.append(ifWasNullPopAndGoto(context, end, double.class, value.getType()));
                    block.invokeStatic(Operations.class, "castToDouble", double.class, value.getType());
                    return typedByteCodeNode(block.visitLabel(end), double.class);
                case "VARCHAR":
                    block.append(ifWasNullPopAndGoto(context, end, Slice.class, value.getType()));
                    block.invokeStatic(Operations.class, "castToSlice", Slice.class, value.getType());
                    return typedByteCodeNode(block.visitLabel(end), Slice.class);
            }
        }
        throw new UnsupportedOperationException("Unsupported type: " + node.getType());
    }

    @Override
    protected TypedByteCodeNode visitArithmeticExpression(ArithmeticExpression node, CompilerContext context)
    {
        TypedByteCodeNode left = process(node.getLeft(), context);
        if (left.getType() == void.class) {
            return left;
        }

        TypedByteCodeNode right = process(node.getRight(), context);
        if (right.getType() == void.class) {
            return right;
        }

        Class<?> type = getType(left, right);
        if (!isNumber(type)) {
            throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType()));
        }

        Block block = new Block(context).comment(node.toString());
        LabelNode end = new LabelNode("end");

        block.append(coerceToType(context, left, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, type, left.getType()));

        block.append(coerceToType(context, right, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, type, type, right.getType()));

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
                throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType()));
        }
        return typedByteCodeNode(block.visitLabel(end), type);
    }

    @Override
    protected TypedByteCodeNode visitNegativeExpression(NegativeExpression node, CompilerContext context)
    {
        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
            return value;
        }

        if (!isNumber(value.getType())) {
            throw new UnsupportedOperationException(format("not yet implemented: negate(%s)", value.getType()));
        }

        // simple single op so there is no reason to do a null check
        Block block = new Block(context)
                .comment(node.toString())
                .append(value.getNode())
                .invokeStatic(Operations.class, "negate", value.getType(), value.getType());

        return typedByteCodeNode(block, value.getType());
    }

    @Override
    protected TypedByteCodeNode visitLogicalBinaryExpression(LogicalBinaryExpression node, CompilerContext context)
    {
        TypedByteCodeNode left = process(node.getLeft(), context);
        if (left.getType() == void.class) {
            left = coerceToType(context, left, boolean.class);
        }
        Preconditions.checkState(left.getType() == boolean.class, "Expected logical binary expression left value to be a boolean but is a %s: %s", left.getType().getName(), node);

        TypedByteCodeNode right = process(node.getRight(), context);
        if (right.getType() == void.class) {
            right = coerceToType(context, right, boolean.class);
        }
        Preconditions.checkState(right.getType() == boolean.class, "Expected logical binary expression right value to be a boolean but is a %s: %s", right.getType().getName(), node);

        switch (node.getType()) {
            case AND:
                return visitAnd(context, left, right, node.toString());
            case OR:
                return visitOr(context, left, right, node.toString());
        }
        throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType()));
    }

    private TypedByteCodeNode visitAnd(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right, String comment)
    {
        Block block = new Block(context)
                .comment(comment)
                .setDescription("AND");

        block.append(left.getNode());

        IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                .comment("if left wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue(new Block(context)
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .putVariable("wasNull", false)
                .pop(left.getType()) // discard left value
                .push(true));

        LabelNode leftIsTrue = new LabelNode("leftIsTrue");
        ifLeftIsNull.ifFalse(new Block(context)
                .comment("if left is false, push false, and goto end")
                .ifTrueGoto(leftIsTrue)
                .push(false)
                .gotoLabel(end)
                .comment("left was true; push left null flag on the stack (false)")
                .visitLabel(leftIsTrue)
                .push(false));

        block.append(ifLeftIsNull.build());

        // At this point we know the left expression was either NULL or TRUE.  The stack contains a single boolean
        // value for this expression which indicates if the left value was NULL.

        // eval right!
        block.append(right.getNode());

        IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                .comment("if right wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue(new Block(context)
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(right.getType()));

        LabelNode rightIsTrue = new LabelNode("rightIsTrue");
        ifRightIsNull.ifFalse(new Block(context)
                .comment("if right is false, pop left null flag off stack, push false and goto end")
                .ifTrueGoto(rightIsTrue)
                .pop(boolean.class)
                .push(false)
                .gotoLabel(end)
                .comment("right was true; store left null flag (on stack) in wasNull variable, and push true")
                .visitLabel(rightIsTrue)
                .putVariable("wasNull")
                .push(true));

        block.append(ifRightIsNull.build())
                .visitLabel(end);

        return typedByteCodeNode(block, boolean.class);
    }

    private TypedByteCodeNode visitOr(CompilerContext context, TypedByteCodeNode left, TypedByteCodeNode right, String comment)
    {
        Block block = new Block(context)
                .comment(comment)
                .setDescription("OR");

        block.append(left.getNode());

        IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                .comment("if left wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue(new Block(context)
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .putVariable("wasNull", false)
                .pop(left.getType()) // discard left value
                .push(true));

        LabelNode leftIsFalse = new LabelNode("leftIsFalse");
        ifLeftIsNull.ifFalse(new Block(context)
                .comment("if left is true, push true, and goto end")
                .ifFalseGoto(leftIsFalse)
                .push(true)
                .gotoLabel(end)
                .comment("left was false; push left null flag on the stack (false)")
                .visitLabel(leftIsFalse)
                .push(false));

        block.append(ifLeftIsNull.build());

        // At this point we know the left expression was either NULL or FALSE.  The stack contains a single boolean
        // value for this expression which indicates if the left value was NULL.

        // eval right!
        block.append(right.getNode());

        IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                .comment("if right wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue(new Block(context)
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(right.getType()));

        LabelNode rightIsTrue = new LabelNode("rightIsTrue");
        ifRightIsNull.ifFalse(new Block(context)
                .comment("if right is true, pop left null flag off stack, push true and goto end")
                .ifFalseGoto(rightIsTrue)
                .pop(boolean.class)
                .push(true)
                .gotoLabel(end)
                .comment("right was false; store left null flag (on stack) in wasNull variable, and push false")
                .visitLabel(rightIsTrue)
                .putVariable("wasNull")
                .push(false));

        block.append(ifRightIsNull.build())
                .visitLabel(end);

        return typedByteCodeNode(block, boolean.class);
    }

    @Override
    protected TypedByteCodeNode visitNotExpression(NotExpression node, CompilerContext context)
    {
        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
            return value;
        }

        Preconditions.checkState(value.getType() == boolean.class);
        // simple single op so there is no reason to do a null check
        return typedByteCodeNode(new Block(context)
                .comment(node.toString())
                .append(value.getNode())
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
        if (left.getType() == void.class) {
            return left;
        }

        TypedByteCodeNode right = process(node.getRight(), context);
        if (right.getType() == void.class) {
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
                checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType());
                function = "lessThan";
                break;
            case LESS_THAN_OR_EQUAL:
                checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType());
                function = "lessThanOrEqual";
                break;
            case GREATER_THAN:
                checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType());
                function = "greaterThan";
                break;
            case GREATER_THAN_OR_EQUAL:
                checkArgument(type != boolean.class, "not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType());
                function = "greaterThanOrEqual";
                break;
            default:
                throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), left.getType(), right.getType()));
        }

        LabelNode end = new LabelNode("end");
        Block block = new Block(context)
                .comment(node.toString());

        block.append(coerceToType(context, left, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, boolean.class, left.getType()));

        block.append(coerceToType(context, right, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, right.getType()));

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
            return typedByteCodeNode(loadBoolean(false), boolean.class);
        }

        Block block = new Block(context)
                .comment(node.toString())
                .comment("left")
                .append(coerceToType(context, left, type).getNode())
                .getVariable("wasNull")
                .comment("clear was null")
                .putVariable("wasNull", false)
                .comment("right")
                .append(coerceToType(context, right, type).getNode())
                .getVariable("wasNull")
                .comment("clear was null")
                .putVariable("wasNull", false)
                .invokeStatic(Operations.class, "isDistinctFrom", boolean.class, type, boolean.class, type, boolean.class);

        return typedByteCodeNode(block, boolean.class);
    }

    @Override
    protected TypedByteCodeNode visitBetweenPredicate(BetweenPredicate node, CompilerContext context)
    {
        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
            return value;
        }

        TypedByteCodeNode min = process(node.getMin(), context);
        if (min.getType() == void.class) {
            return min;
        }

        TypedByteCodeNode max = process(node.getMax(), context);
        if (max.getType() == void.class) {
            return max;
        }

        Class<?> type = getType(value, min, max);

        LabelNode end = new LabelNode("end");
        Block block = new Block(context)
                .comment(node.toString());

        block.append(coerceToType(context, value, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, boolean.class, type));

        block.append(coerceToType(context, min, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, type));

        block.append(coerceToType(context, max, type).getNode());
        block.append(ifWasNullPopAndGoto(context, end, boolean.class, type, type, type));

        block.invokeStatic(Operations.class, "between", boolean.class, type, type, type);
        return typedByteCodeNode(block.visitLabel(end), boolean.class);
    }

    @Override
    protected TypedByteCodeNode visitIsNotNullPredicate(IsNotNullPredicate node, CompilerContext context)
    {
        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
            return typedByteCodeNode(loadBoolean(false), boolean.class);
        }

        // evaluate the expression, pop the produced value, load the null flag, and invert it
        Block block = new Block(context)
                .comment(node.toString())
                .append(value.getNode())
                .pop(value.getType())
                .getVariable("wasNull")
                .invokeStatic(Operations.class, "not", boolean.class, boolean.class);

        // clear the null flag
        block.putVariable("wasNull", false);

        return typedByteCodeNode(block, boolean.class);
    }

    @Override
    protected TypedByteCodeNode visitIsNullPredicate(IsNullPredicate node, CompilerContext context)
    {
        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
            return typedByteCodeNode(loadBoolean(true), boolean.class);
        }

        // evaluate the expression, pop the produced value, and load the null flag
        Block block = new Block(context)
                .comment(node.toString())
                .append(value.getNode())
                .pop(value.getType())
                .getVariable("wasNull");

        // clear the null flag
        block.putVariable("wasNull", false);

        return typedByteCodeNode(block, boolean.class);
    }

    @Override
    protected TypedByteCodeNode visitIfExpression(IfExpression node, CompilerContext context)
    {
        TypedByteCodeNode conditionValue = process(node.getCondition(), context);
        TypedByteCodeNode trueValue = process(node.getTrueValue(), context);
        TypedByteCodeNode falseValue = process(node.getFalseValue().or(new NullLiteral()), context);

        if (conditionValue.getType() == void.class) {
            return falseValue;
        }
        Preconditions.checkState(conditionValue.getType() == boolean.class);

        // if conditionValue and conditionValue was not null
        Block condition = new Block(context)
                .comment(node.toString())
                .append(conditionValue.getNode())
                .comment("... and condition value was not null")
                .getVariable("wasNull")
                .invokeStatic(Operations.class, "not", boolean.class, boolean.class)
                .invokeStatic(Operations.class, "and", boolean.class, boolean.class, boolean.class)
                .putVariable("wasNull", false);

        Class<?> type = getType(trueValue, falseValue);
        if (type == void.class) {
            // both true and false are null literal
            return trueValue;
        }

        trueValue = coerceToType(context, trueValue, type);
        falseValue = coerceToType(context, falseValue, type);

        return typedByteCodeNode(new IfStatement(context, condition, trueValue.getNode(), falseValue.getNode()), type);
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
            if (whenClause.condition.getType() == void.class) {
                continue;
            }
            Preconditions.checkState(whenClause.condition.getType() == boolean.class);

            // if conditionValue and conditionValue was not null
            Block condition = new Block(context)
                    .append(whenClause.condition.getNode())
                    .comment("... and condition value was not null")
                    .getVariable("wasNull")
                    .invokeStatic(Operations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(Operations.class, "and", boolean.class, boolean.class, boolean.class)
                    .putVariable("wasNull", false);

            elseValue = typedByteCodeNode(new IfStatement(context, condition, coerceToType(context, whenClause.value, type).getNode(), elseValue.getNode()), type);
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

        if (value.getType() == void.class) {
            return coerceToType(context, elseValue, resultType);
        }

        // evaluate the value and store it in a variable
        LabelNode nullValue = new LabelNode("nullCondition");
        Variable tempVariable = context.createTempVariable(valueType);
        Block block = new Block(context)
                .append(coerceToType(context, value, valueType).getNode())
                .append(ifWasNullClearPopAndGoto(context, nullValue, void.class, valueType))
                .putVariable(tempVariable.getLocalVariableDefinition());

        // build the statements
        elseValue = typedByteCodeNode(new Block(context).visitLabel(nullValue).append(coerceToType(context, elseValue, resultType).getNode()), resultType);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (TypedWhenClause whenClause : Lists.reverse(new ArrayList<>(whenClauses))) {
            LabelNode nullCondition = new LabelNode("nullCondition");
            Block condition = new Block(context)
                    .append(coerceToType(context, whenClause.condition, valueType).getNode())
                    .append(ifWasNullPopAndGoto(context, nullCondition, boolean.class, valueType))
                    .getVariable(tempVariable.getLocalVariableDefinition())
                    .invokeStatic(Operations.class, "equal", boolean.class, valueType, valueType)
                    .visitLabel(nullCondition)
                    .putVariable("wasNull", false);

            elseValue = typedByteCodeNode(new IfStatement(context,
                    format("when %s", whenClause),
                    condition,
                    coerceToType(context, whenClause.value, resultType).getNode(),
                    elseValue.getNode()), resultType);
        }

        return typedByteCodeNode(block.append(elseValue.getNode()), resultType);
    }

    @Override
    protected TypedByteCodeNode visitNullIfExpression(NullIfExpression node, CompilerContext context)
    {
        TypedByteCodeNode first = process(node.getFirst(), context);
        TypedByteCodeNode second = process(node.getSecond(), context);
        if (first.getType() == void.class) {
            return first;
        }

        Class<?> comparisonType = getType(first, second);

        LabelNode notMatch = new LabelNode("notMatch");
        Block block = new Block(context)
                .comment(node.toString())
                .append(first.getNode())
                .append(ifWasNullPopAndGoto(context, notMatch, void.class))
                .append(coerceToType(context, typedByteCodeNode(new Block(context).dup(first.getType()), first.getType()), comparisonType).getNode())
                .append(coerceToType(context, second, comparisonType).getNode())
                .append(ifWasNullClearPopAndGoto(context, notMatch, void.class, comparisonType, comparisonType));

        Block conditionBlock = new Block(context)
                .invokeStatic(Operations.class, "equal", boolean.class, comparisonType, comparisonType);

        Block trueBlock = new Block(context)
                .putVariable("wasNull", true)
                .pop(first.getType())
                .pushJavaDefault(first.getType());

        block.append(new IfStatement(context, conditionBlock, trueBlock, notMatch));

        return typedByteCodeNode(block, first.getType());
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
                    .append(coerceToType(context, operand, type).getNode())
                    .getVariable("wasNull");

            // if value was null, pop the null value, clear the null flag, and process the next operand
            Block nullBlock = new Block(context)
                    .pop(type)
                    .putVariable("wasNull", false)
                    .append(nullValue.getNode());

            nullValue = typedByteCodeNode(new IfStatement(context, condition, nullBlock, NOP), type);
        }

        return typedByteCodeNode(nullValue.getNode(), type);
    }

    @Override
    protected TypedByteCodeNode visitInPredicate(InPredicate node, CompilerContext context)
    {
        Expression valueListExpression = node.getValueList();
        if (!(valueListExpression instanceof InListExpression)) {
            throw new UnsupportedOperationException("Compilation of IN subquery is not supported yet");
        }

        TypedByteCodeNode value = process(node.getValue(), context);
        if (value.getType() == void.class) {
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
            if (testNode.getNode() instanceof Constant) {
                Constant constant = (Constant) testNode.getNode();
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
                }
                else {
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
                .append(coerceToType(context, value, type).getNode())
                .append(ifWasNullPopAndGoto(context, end, boolean.class, type))
                .append(switchBlock)
                .append(defaultCaseBlock);

        Block matchBlock = new Block(context)
                .setDescription("match")
                .visitLabel(match)
                .pop(type)
                .putVariable("wasNull", false)
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        Block noMatchBlock = new Block(context)
                .setDescription("noMatch")
                .visitLabel(noMatch)
                .pop(type)
                .push(false)
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
            caseBlock.putVariable(caseWasNull.getLocalVariableDefinition(), false);
        }

        LabelNode elseLabel = new LabelNode("else");
        Block elseBlock = new Block(context)
                .visitLabel(elseLabel);

        if (checkForNulls) {
            elseBlock.getVariable(caseWasNull.getLocalVariableDefinition())
                    .putVariable("wasNull");
        }

        elseBlock.gotoLabel(noMatchLabel);

        ByteCodeNode elseNode = elseBlock;
        for (TypedByteCodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatementBuilder test = ifStatementBuilder(context);

            Block condition = new Block(context)
                    .visitLabel(testLabel)
                    .dup(type)
                    .append(coerceToType(context, testNode, type).getNode());

            if (checkForNulls) {
                condition.getVariable("wasNull")
                        .putVariable(caseWasNull.getLocalVariableDefinition())
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

    private ByteCodeNode handleNullValue(CompilerContext context,
            LabelNode label,
            Class<?> returnType,
            List<? extends Class<?>> stackArgsToPop,
            boolean clearNullFlag)
    {
        Block nullCheck = new Block(context)
                .setDescription("ifWasNullGoto")
                .getVariable("wasNull");

        String clearComment = null;
        if (clearNullFlag) {
            nullCheck.putVariable("wasNull", false);
            clearComment = "clear wasNull";
        }

        Block isNull = new Block(context);
        for (Class<?> parameterType : stackArgsToPop) {
            isNull.pop(parameterType);
        }

        isNull.pushJavaDefault(returnType);
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
        if (node.getType() == void.class) {
            return typedByteCodeNode(new Block(context).append(node.getNode()).pushJavaDefault(type), type);
        }
        if (node.getType() == long.class && type == double.class) {
            return typedByteCodeNode(new Block(context).append(node.getNode()).append(L2D), type);
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
                return node.getType();
            }
        };
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
            catch (ReflectiveOperationException e) {
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
