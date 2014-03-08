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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.control.IfStatement.IfStatementBuilder;
import com.facebook.presto.byteCode.control.LookupSwitch.LookupSwitchBuilder;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.byteCode.instruction.VariableInstruction;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorInfo.OperatorType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
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

import static com.facebook.presto.byteCode.OpCodes.NOP;
import static com.facebook.presto.byteCode.control.IfStatement.ifStatementBuilder;
import static com.facebook.presto.byteCode.control.LookupSwitch.lookupSwitchBuilder;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.JumpInstruction.jump;
import static com.facebook.presto.sql.gen.SliceConstant.sliceConstant;
import static com.facebook.presto.type.NullType.NULL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class ByteCodeExpressionVisitor
        extends AstVisitor<ByteCodeNode, CompilerContext>
{
    private final Metadata metadata;
    private final BootstrapFunctionBinder bootstrapFunctionBinder;
    private final Map<Expression, Type> expressionTypes;
    private final ByteCodeNode getSessionByteCode;
    private final boolean sourceIsCursor;

    public ByteCodeExpressionVisitor(
            Metadata metadata,
            BootstrapFunctionBinder bootstrapFunctionBinder,
            Map<Expression, Type> expressionTypes,
            ByteCodeNode getSessionByteCode,
            boolean sourceIsCursor)
    {
        this.metadata = metadata;
        this.bootstrapFunctionBinder = bootstrapFunctionBinder;
        this.expressionTypes = expressionTypes;
        this.getSessionByteCode = getSessionByteCode;
        this.sourceIsCursor = sourceIsCursor;
    }

    @Override
    protected ByteCodeNode visitBooleanLiteral(BooleanLiteral node, CompilerContext context)
    {
        return loadBoolean(node.getValue());
    }

    @Override
    protected ByteCodeNode visitLongLiteral(LongLiteral node, CompilerContext context)
    {
        return loadLong(node.getValue());
    }

    @Override
    protected ByteCodeNode visitDoubleLiteral(DoubleLiteral node, CompilerContext context)
    {
        return loadDouble(node.getValue());
    }

    @Override
    protected ByteCodeNode visitStringLiteral(StringLiteral node, CompilerContext context)
    {
        return sliceConstant(node.getSlice());
    }

    @Override
    protected ByteCodeNode visitNullLiteral(NullLiteral node, CompilerContext context)
    {
        return new Block(context).putVariable("wasNull", true);
    }

    @Override
    public ByteCodeNode visitInputReference(InputReference node, CompilerContext context)
    {
        Input input = node.getInput();
        int channel = input.getChannel();

        Type type = expressionTypes.get(node);
        checkState(type != null, "No type for input %s", input);
        Class<?> javaType = type.getJavaType();

        if (sourceIsCursor) {
            Block isNullCheck = new Block(context)
                    .setDescription(format("cursor.get%s(%d)", type, channel))
                    .getVariable("cursor")
                    .push(channel)
                    .invokeInterface(RecordCursor.class, "isNull", boolean.class, int.class);

            Block isNull = new Block(context)
                    .putVariable("wasNull", true)
                    .pushJavaDefault(javaType);
            if (javaType == boolean.class) {
                Block isNotNull = new Block(context)
                        .getVariable("cursor")
                        .push(channel)
                        .invokeInterface(RecordCursor.class, "getBoolean", boolean.class, int.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == long.class) {
                Block isNotNull = new Block(context)
                        .getVariable("cursor")
                        .push(channel)
                        .invokeInterface(RecordCursor.class, "getLong", long.class, int.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == double.class) {
                Block isNotNull = new Block(context)
                        .getVariable("cursor")
                        .push(channel)
                        .invokeInterface(RecordCursor.class, "getDouble", double.class, int.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == Slice.class) {
                Block isNotNull = new Block(context)
                        .getVariable("cursor")
                        .push(channel)
                        .invokeInterface(RecordCursor.class, "getString", byte[].class, int.class)
                        .invokeStatic(Slices.class, "wrappedBuffer", Slice.class, byte[].class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else {
                throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
        else {
            Block isNullCheck = new Block(context)
                    .setDescription(format("channel_%d.get%s()", channel, type))
                    .getVariable("channel_" + channel)
                    .invokeInterface(BlockCursor.class, "isNull", boolean.class);

            Block isNull = new Block(context)
                    .putVariable("wasNull", true)
                    .pushJavaDefault(javaType);
            if (javaType == boolean.class) {
                Block isNotNull = new Block(context)
                        .getVariable("channel_" + channel)
                        .invokeInterface(BlockCursor.class, "getBoolean", boolean.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == long.class) {
                Block isNotNull = new Block(context)
                        .getVariable("channel_" + channel)
                        .invokeInterface(BlockCursor.class, "getLong", long.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == double.class) {
                Block isNotNull = new Block(context)
                        .getVariable("channel_" + channel)
                        .invokeInterface(BlockCursor.class, "getDouble", double.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else if (javaType == Slice.class) {
                Block isNotNull = new Block(context)
                        .getVariable("channel_" + channel)
                        .invokeInterface(BlockCursor.class, "getSlice", Slice.class);
                return new IfStatement(context, isNullCheck, isNull, isNotNull);
            }
            else {
                throw new UnsupportedOperationException("not yet implemented: " + type);
            }
        }
    }

    @Override
    protected ByteCodeNode visitFunctionCall(FunctionCall node, CompilerContext context)
    {
        List<ByteCodeNode> arguments = new ArrayList<>();
        List<Type> argumentTypes = new ArrayList<>();
        for (Expression argument : node.getArguments()) {
            arguments.add(process(argument, context));
            argumentTypes.add(expressionTypes.get(argument));
        }

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(node.getName(), getSessionByteCode, arguments, argumentTypes);
        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    @Override
    protected ByteCodeNode visitLikePredicate(LikePredicate node, CompilerContext context)
    {
        ImmutableList<Expression> expressions;
        if (node.getEscape() != null) {
            expressions = ImmutableList.of(node.getValue(), node.getPattern(), node.getEscape());
        }
        else {
            expressions = ImmutableList.of(node.getValue(), node.getPattern());
        }

        List<ByteCodeNode> arguments = new ArrayList<>();
        for (Expression argument : expressions) {
            arguments.add(process(argument, context));
        }

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction("like", getSessionByteCode, arguments, new LikeFunctionBinder());
        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    private ByteCodeNode visitFunctionBinding(CompilerContext context, FunctionBinding functionBinding, String comment)
    {
        List<ByteCodeNode> arguments = functionBinding.getArguments();
        MethodType methodType = functionBinding.getCallSite().type();
        Class<?> unboxedReturnType = Primitives.unwrap(methodType.returnType());

        LabelNode end = new LabelNode("end");
        Block block = new Block(context)
                .setDescription("invoke")
                .comment(comment);
        ArrayList<Class<?>> stackTypes = new ArrayList<>();
        for (int i = 0; i < arguments.size(); i++) {
            block.append(arguments.get(i));
            stackTypes.add(methodType.parameterType(i));
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

        return block;
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
    public ByteCodeNode visitCast(Cast node, CompilerContext context)
    {
        Type type = metadata.getType(node.getType());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported type: " + node.getType());
        }

        if (expressionTypes.get(node.getExpression()).equals(NULL)) {
            // set was null and push java default
            return new Block(context).putVariable("wasNull", true).pushJavaDefault(type.getJavaType());
        }

        ByteCodeNode value = process(node.getExpression(), context);
        FunctionBinding functionBinding = bootstrapFunctionBinder.bindCastOperator(
                getSessionByteCode,
                value,
                expressionTypes.get(node.getExpression()),
                type);

        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    @Override
    protected ByteCodeNode visitArithmeticExpression(ArithmeticExpression node, CompilerContext context)
    {
        ByteCodeNode left = process(node.getLeft(), context);
        ByteCodeNode right = process(node.getRight(), context);

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.valueOf(node.getType().name()),
                getSessionByteCode,
                ImmutableList.of(left, right),
                types(node.getLeft(), node.getRight()));

        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    @Override
    protected ByteCodeNode visitNegativeExpression(NegativeExpression node, CompilerContext context)
    {
        ByteCodeNode value = process(node.getValue(), context);

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.NEGATION,
                getSessionByteCode,
                ImmutableList.of(value),
                types(node.getValue()));

        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    @Override
    protected ByteCodeNode visitLogicalBinaryExpression(LogicalBinaryExpression node, CompilerContext context)
    {
        ByteCodeNode left = process(node.getLeft(), context);
        Type leftType = expressionTypes.get(node.getLeft());
        ByteCodeNode right = process(node.getRight(), context);
        Type rightType = expressionTypes.get(node.getRight());

        switch (node.getType()) {
            case AND:
                return visitAnd(context, left, leftType, right, rightType, node.toString());
            case OR:
                return visitOr(context, left, leftType, right, rightType, node.toString());
        }
        throw new UnsupportedOperationException(format("not yet implemented: %s(%s, %s)", node.getType(), leftType, rightType));
    }

    private ByteCodeNode visitAnd(CompilerContext context, ByteCodeNode left, Type leftType, ByteCodeNode right, Type rightType, String comment)
    {
        Block block = new Block(context)
                .comment(comment)
                .setDescription("AND");

        block.append(left);

        IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                .comment("if left wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue(new Block(context)
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .putVariable("wasNull", false)
                .pop(leftType.getJavaType()) // discard left value
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
        block.append(right);

        IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                .comment("if right wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue(new Block(context)
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(rightType.getJavaType()));

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

        return block;
    }

    private ByteCodeNode visitOr(CompilerContext context, ByteCodeNode left, Type leftType, ByteCodeNode right, Type rightType, String comment)
    {
        Block block = new Block(context)
                .comment(comment)
                .setDescription("OR");

        block.append(left);

        IfStatementBuilder ifLeftIsNull = ifStatementBuilder(context)
                .comment("if left wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        LabelNode end = new LabelNode("end");
        ifLeftIsNull.ifTrue(new Block(context)
                .comment("clear the null flag, pop left value off stack, and push left null flag on the stack (true)")
                .putVariable("wasNull", false)
                .pop(leftType.getJavaType()) // discard left value
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
        block.append(right);

        IfStatementBuilder ifRightIsNull = ifStatementBuilder(context)
                .comment("if right wasNull...")
                .condition(new Block(context).getVariable("wasNull"));

        // this leaves a single boolean on the stack which is ignored since the value in NULL
        ifRightIsNull.ifTrue(new Block(context)
                .comment("right was null, pop the right value off the stack; wasNull flag remains set to TRUE")
                .pop(rightType.getJavaType()));

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

        return block;
    }

    @Override
    protected ByteCodeNode visitNotExpression(NotExpression node, CompilerContext context)
    {
        ByteCodeNode value = process(node.getValue(), context);

        // simple single op so there is no reason to do a null check
        return new Block(context)
                .comment(node.toString())
                .append(value)
                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class);
    }

    @Override
    protected ByteCodeNode visitComparisonExpression(ComparisonExpression node, CompilerContext context)
    {
        // distinct requires special null handling rules
        if (node.getType() == ComparisonExpression.Type.IS_DISTINCT_FROM) {
            return visitIsDistinctFrom(node, context);
        }

        ByteCodeNode left = process(node.getLeft(), context);
        ByteCodeNode right = process(node.getRight(), context);

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.valueOf(node.getType().name()),
                getSessionByteCode,
                ImmutableList.of(left, right),
                types(node.getLeft(), node.getRight()));

        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    private ByteCodeNode visitIsDistinctFrom(ComparisonExpression node, CompilerContext context)
    {
        Type leftType = expressionTypes.get(node.getLeft());
        Type rightType = expressionTypes.get(node.getRight());

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.EQUAL,
                getSessionByteCode,
                ImmutableList.<ByteCodeNode>of(NOP, NOP),
                ImmutableList.of(leftType, rightType));

        ByteCodeNode equalsCall = new Block(context).comment("equals(%s, %s)", leftType, rightType)
                .invokeDynamic(functionBinding.getName(), functionBinding.getCallSite().type(), functionBinding.getBindingId());

        Block block = new Block(context)
                .comment(node.toString())
                .comment("left")
                .append(process(node.getLeft(), context))
                .append(new IfStatement(context,
                        new Block(context).getVariable("wasNull"),
                        new Block(context)
                                .pop(leftType.getJavaType())
                                .putVariable("wasNull", false)
                                .comment("right is not null")
                                .append(process(node.getRight(), context))
                                .pop(rightType.getJavaType())
                                .getVariable("wasNull")
                                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class),
                        new Block(context)
                                .comment("right")
                                .append(process(node.getRight(), context))
                                .append(new IfStatement(context,
                                        new Block(context).getVariable("wasNull"),
                                        new Block(context)
                                                .pop(leftType.getJavaType())
                                                .pop(rightType.getJavaType())
                                                .push(true),
                                        new Block(context)
                                                .append(equalsCall)
                                                .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)))))
                .putVariable("wasNull", false);

        return block;
    }

    @Override
    protected ByteCodeNode visitBetweenPredicate(BetweenPredicate node, CompilerContext context)
    {
        ByteCodeNode value = process(node.getValue(), context);
        ByteCodeNode min = process(node.getMin(), context);
        ByteCodeNode max = process(node.getMax(), context);

        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.BETWEEN,
                getSessionByteCode,
                ImmutableList.of(value, min, max),
                types(node.getValue(), node.getMin(), node.getMax()));

        return visitFunctionBinding(context, functionBinding, node.toString());
    }

    @Override
    protected ByteCodeNode visitIsNullPredicate(IsNullPredicate node, CompilerContext context)
    {
        Type valueType = expressionTypes.get(node.getValue());
        if (valueType.equals(NULL)) {
            return loadBoolean(true);
        }

        ByteCodeNode value = process(node.getValue(), context);

        // evaluate the expression, pop the produced value, and load the null flag
        Block block = new Block(context)
                .comment(node.toString())
                .append(value)
                .pop(valueType.getJavaType())
                .getVariable("wasNull");

        // clear the null flag
        block.putVariable("wasNull", false);

        return block;
    }

    @Override
    protected ByteCodeNode visitSearchedCaseExpression(SearchedCaseExpression node, final CompilerContext context)
    {
        Type type = expressionTypes.get(node);
        ByteCodeNode elseValue;
        if (node.getDefaultValue() != null) {
            elseValue = process(node.getDefaultValue(), context);
        }
        else {
            elseValue = typedNull(context, type.getJavaType());
        }

        List<TypedWhenClause> whenClauses = ImmutableList.copyOf(transform(node.getWhenClauses(), new Function<WhenClause, TypedWhenClause>()
        {
            @Override
            public TypedWhenClause apply(WhenClause whenClause)
            {
                return new TypedWhenClause(context, whenClause);
            }
        }));

        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (TypedWhenClause whenClause : Lists.reverse(new ArrayList<>(whenClauses))) {
            // if conditionValue and conditionValue was not null
            Block condition = new Block(context)
                    .append(whenClause.operandBlock)
                    .comment("... and condition value was not null")
                    .getVariable("wasNull")
                    .invokeStatic(CompilerOperations.class, "not", boolean.class, boolean.class)
                    .invokeStatic(CompilerOperations.class, "and", boolean.class, boolean.class, boolean.class)
                    .putVariable("wasNull", false);

            elseValue = new IfStatement(context, condition, whenClause.valueBlock, elseValue);
        }

        return elseValue;
    }

    @Override
    protected ByteCodeNode visitSimpleCaseExpression(SimpleCaseExpression node, final CompilerContext context)
    {
        // process value, else, and all when clauses
        ByteCodeNode value = process(node.getOperand(), context);
        Type type = expressionTypes.get(node);
        ByteCodeNode elseValue;
        if (node.getDefaultValue() != null) {
            elseValue = process(node.getDefaultValue(), context);
        }
        else {
            elseValue = typedNull(context, type.getJavaType());
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
        Class<?> valueType = expressionTypes.get(node.getOperand()).getJavaType();

        // evaluate the value and store it in a variable
        LabelNode nullValue = new LabelNode("nullCondition");
        Variable tempVariable = context.createTempVariable(valueType);
        Block block = new Block(context)
                .append(value)
                .append(ifWasNullClearPopAndGoto(context, nullValue, void.class, valueType))
                .putVariable(tempVariable.getLocalVariableDefinition());

        ByteCodeNode getTempVariableNode = VariableInstruction.loadVariable(tempVariable.getLocalVariableDefinition());

        // build the statements
        elseValue = new Block(context).visitLabel(nullValue).append(elseValue);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (TypedWhenClause whenClause : Lists.reverse(new ArrayList<>(whenClauses))) {
            FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                    OperatorType.EQUAL,
                    getSessionByteCode,
                    ImmutableList.of(whenClause.operandBlock, getTempVariableNode),
                    types(whenClause.operand, node.getOperand()));
            ByteCodeNode equalsCall = visitFunctionBinding(context, functionBinding, whenClause.operand.toString());

            Block condition = new Block(context)
                    .append(equalsCall)
                    .putVariable("wasNull", false);

            elseValue = new IfStatement(context,
                    format("when %s", whenClause.operand),
                    condition,
                    whenClause.valueBlock,
                    elseValue);
        }

        return block.append(elseValue);
    }

    @Override
    protected ByteCodeNode visitNullIfExpression(NullIfExpression node, CompilerContext context)
    {
        ByteCodeNode first = process(node.getFirst(), context);
        Type firstType = expressionTypes.get(node.getFirst());
        ByteCodeNode second = process(node.getSecond(), context);

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        Block block = new Block(context)
                .comment(node.toString())
                .append(first)
                .append(ifWasNullPopAndGoto(context, notMatch, void.class));

        // if (equal(dupe(first), second)
        FunctionBinding functionBinding = bootstrapFunctionBinder.bindOperator(
                OperatorType.EQUAL,
                getSessionByteCode,
                ImmutableList.of(new Block(context).dup(firstType.getJavaType()), second),
                types(node.getFirst(), node.getSecond()));
        ByteCodeNode equalsCall = visitFunctionBinding(context, functionBinding, "equal");

        Block conditionBlock = new Block(context)
                .append(equalsCall)
                .append(ifWasNullClearPopAndGoto(context, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        Block trueBlock = new Block(context)
                .putVariable("wasNull", true)
                .pop(firstType.getJavaType())
                .pushJavaDefault(firstType.getJavaType());

        // else return first (which is still on the stack
        block.append(new IfStatement(context, conditionBlock, trueBlock, notMatch));

        return block;
    }

    @Override
    protected ByteCodeNode visitCoalesceExpression(CoalesceExpression node, CompilerContext context)
    {
        List<ByteCodeNode> operands = new ArrayList<>();
        for (Expression expression : node.getOperands()) {
            operands.add(process(expression, context));
        }

        Class<?> type = expressionTypes.get(node).getJavaType();

        ByteCodeNode nullValue = typedNull(context, type);
        // reverse list because current if statement builder doesn't support if/else so we need to build the if statements bottom up
        for (ByteCodeNode operand : Lists.reverse(operands)) {
            Block condition = new Block(context)
                    .append(operand)
                    .getVariable("wasNull");

            // if value was null, pop the null value, clear the null flag, and process the next operand
            Block nullBlock = new Block(context)
                    .pop(type)
                    .putVariable("wasNull", false)
                    .append(nullValue);

            nullValue = new IfStatement(context, condition, nullBlock, NOP);
        }

        return nullValue;
    }

    @Override
    protected ByteCodeNode visitInPredicate(InPredicate node, CompilerContext context)
    {
        Expression valueListExpression = node.getValueList();
        if (!(valueListExpression instanceof InListExpression)) {
            throw new UnsupportedOperationException("Compilation of IN subquery is not supported yet");
        }

        ByteCodeNode value = process(node.getValue(), context);

        ImmutableList.Builder<ByteCodeNode> values = ImmutableList.builder();
        InListExpression valueList = (InListExpression) valueListExpression;
        for (Expression test : valueList.getValues()) {
            ByteCodeNode testNode = process(test, context);
            values.add(testNode);
        }

        Type type = expressionTypes.get(node.getValue());
        Class<?> javaType = type.getJavaType();

        FunctionBinding hashCodeFunction = bootstrapFunctionBinder.bindOperator(
                OperatorType.HASH_CODE,
                getSessionByteCode,
                ImmutableList.<ByteCodeNode>of(NOP),
                ImmutableList.of(type));

        ImmutableListMultimap.Builder<Integer, ByteCodeNode> hashBucketsBuilder = ImmutableListMultimap.builder();
        ImmutableList.Builder<ByteCodeNode> defaultBucket = ImmutableList.builder();
        ImmutableSet.Builder<Object> constantValuesBuilder = ImmutableSet.builder();
        for (ByteCodeNode testNode : values.build()) {
            if (testNode instanceof Constant) {
                Constant constant = (Constant) testNode;
                Object testValue = constant.getValue();
                constantValuesBuilder.add(testValue);

                if (javaType == boolean.class) {
                    // boolean constant is actually an integer type
                    testValue = ((Number) testValue).intValue() != 0;
                }

                int hashCode;
                try {
                    hashCode = (int) hashCodeFunction.getCallSite().dynamicInvoker().invoke(testValue);
                }
                catch (Throwable throwable) {
                    throw new IllegalArgumentException("Error processing IN statement: error calculating hash code for " + testValue, throwable);
                }

                hashBucketsBuilder.put(hashCode, testNode);
            }
            else {
                defaultBucket.add(testNode);
            }
        }
        ImmutableListMultimap<Integer, ByteCodeNode> hashBuckets = hashBucketsBuilder.build();
        ImmutableSet<Object> constantValues = constantValuesBuilder.build();

        LabelNode end = new LabelNode("end");
        LabelNode match = new LabelNode("match");
        LabelNode noMatch = new LabelNode("noMatch");

        LabelNode defaultLabel = new LabelNode("default");

        ByteCodeNode switchBlock;
        if (constantValues.size() < 1000) {
            Block switchCaseBlocks = new Block(context);
            LookupSwitchBuilder switchBuilder = lookupSwitchBuilder();
            for (Entry<Integer, Collection<ByteCodeNode>> bucket : hashBuckets.asMap().entrySet()) {
                LabelNode label = new LabelNode("inHash" + bucket.getKey());
                switchBuilder.addCase(bucket.getKey(), label);
                Collection<ByteCodeNode> testValues = bucket.getValue();

                Block caseBlock = buildInCase(context, type, label, match, defaultLabel, testValues, false);
                switchCaseBlocks
                        .append(caseBlock.setDescription("case " + bucket.getKey()));
            }
            switchBuilder.defaultCase(defaultLabel);

            switchBlock = new Block(context)
                    .comment("lookupSwitch(hashCode(<stackValue>))")
                    .dup(javaType)
                    .invokeDynamic(hashCodeFunction.getName(), hashCodeFunction.getCallSite().type(), hashCodeFunction.getBindingId())
                    .append(switchBuilder.build())
                    .append(switchCaseBlocks);
        }
        else {
            // for huge IN lists, use a Set
            FunctionBinding functionBinding = bootstrapFunctionBinder.bindFunction(
                    "in",
                    getSessionByteCode,
                    ImmutableList.<ByteCodeNode>of(),
                    new InFunctionBinder(javaType, constantValues));

            switchBlock = new Block(context)
                    .comment("inListSet.contains(<stackValue>)")
                    .append(new IfStatement(context,
                            new Block(context).dup(javaType).invokeDynamic(functionBinding.getName(), functionBinding.getCallSite().type(), functionBinding.getBindingId()),
                            jump(match),
                            NOP));
        }

        Block defaultCaseBlock = buildInCase(context, type, defaultLabel, match, noMatch, defaultBucket.build(), true).setDescription("default");

        Block block = new Block(context)
                .comment(node.toString())
                .append(value)
                .append(ifWasNullPopAndGoto(context, end, boolean.class, javaType))
                .append(switchBlock)
                .append(defaultCaseBlock);

        Block matchBlock = new Block(context)
                .setDescription("match")
                .visitLabel(match)
                .pop(javaType)
                .putVariable("wasNull", false)
                .push(true)
                .gotoLabel(end);
        block.append(matchBlock);

        Block noMatchBlock = new Block(context)
                .setDescription("noMatch")
                .visitLabel(noMatch)
                .pop(javaType)
                .push(false)
                .gotoLabel(end);
        block.append(noMatchBlock);

        block.visitLabel(end);

        return block;
    }

    private Block buildInCase(CompilerContext context,
            Type type,
            LabelNode caseLabel,
            LabelNode matchLabel,
            LabelNode noMatchLabel,
            Collection<ByteCodeNode> testValues,
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

        FunctionBinding equalsFunction = bootstrapFunctionBinder.bindOperator(
                OperatorType.EQUAL,
                getSessionByteCode,
                ImmutableList.<ByteCodeNode>of(NOP, NOP),
                ImmutableList.of(type, type));

        ByteCodeNode elseNode = elseBlock;
        for (ByteCodeNode testNode : testValues) {
            LabelNode testLabel = new LabelNode("test");
            IfStatementBuilder test = ifStatementBuilder(context);

            Block condition = new Block(context)
                    .visitLabel(testLabel)
                    .dup(type.getJavaType())
                    .append(testNode);

            if (checkForNulls) {
                condition.getVariable("wasNull")
                        .putVariable(caseWasNull.getLocalVariableDefinition())
                        .append(ifWasNullPopAndGoto(context, elseLabel, void.class, type.getJavaType(), type.getJavaType()));
            }
            condition.invokeDynamic(equalsFunction.getName(), equalsFunction.getCallSite().type(), equalsFunction.getBindingId());
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
    protected ByteCodeNode visitExpression(Expression node, CompilerContext context)
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

    private ByteCodeNode typedNull(CompilerContext context, Class<?> type)
    {
        return new Block(context).putVariable("wasNull", true).pushJavaDefault(type);
    }

    private List<Type> types(Expression... types)
    {
        return ImmutableList.copyOf(transform(ImmutableList.copyOf(types), Functions.forMap(expressionTypes)));
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
        public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments)
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
        private final Expression operand;
        private final ByteCodeNode operandBlock;
        private final Expression result;
        private final ByteCodeNode valueBlock;

        private TypedWhenClause(CompilerContext context, WhenClause whenClause)
        {
            operand = whenClause.getOperand();
            this.operandBlock = process(operand, context);
            result = whenClause.getResult();
            this.valueBlock = process(result, context);
        }
    }
}
