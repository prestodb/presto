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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.OpCode.DCMPG;
import static com.facebook.presto.byteCode.OpCode.DCMPL;
import static com.facebook.presto.byteCode.OpCode.FCMPG;
import static com.facebook.presto.byteCode.OpCode.FCMPL;
import static com.facebook.presto.byteCode.OpCode.IFEQ;
import static com.facebook.presto.byteCode.OpCode.IFGE;
import static com.facebook.presto.byteCode.OpCode.IFGT;
import static com.facebook.presto.byteCode.OpCode.IFLE;
import static com.facebook.presto.byteCode.OpCode.IFLT;
import static com.facebook.presto.byteCode.OpCode.IFNE;
import static com.facebook.presto.byteCode.OpCode.IF_ACMPEQ;
import static com.facebook.presto.byteCode.OpCode.IF_ACMPNE;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPEQ;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPGE;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPGT;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPLE;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPLT;
import static com.facebook.presto.byteCode.OpCode.IF_ICMPNE;
import static com.facebook.presto.byteCode.OpCode.LCMP;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class ComparisonByteCodeExpression
        extends ByteCodeExpression
{
    static ByteCodeExpression lessThan(ByteCodeExpression left, ByteCodeExpression right)
    {
        checkArgumentTypes(left, right);

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPGE;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFGE;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPG;
            noMatchJumpInstruction = IFGE;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPG;
            noMatchJumpInstruction = IFGE;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }

        return new ComparisonByteCodeExpression("<", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    static ByteCodeExpression greaterThan(ByteCodeExpression left, ByteCodeExpression right)
    {
        checkArgumentTypes(left, right);

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPLE;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFLE;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPL;
            noMatchJumpInstruction = IFLE;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPL;
            noMatchJumpInstruction = IFLE;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }
        return new ComparisonByteCodeExpression(">", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    static ByteCodeExpression lessThanOrEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        checkArgumentTypes(left, right);

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPGT;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFGT;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPG;
            noMatchJumpInstruction = IFGT;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPG;
            noMatchJumpInstruction = IFGT;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }
        return new ComparisonByteCodeExpression("<=", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    static ByteCodeExpression greaterThanOrEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        checkArgumentTypes(left, right);

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPLT;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFLT;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPL;
            noMatchJumpInstruction = IFLT;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPL;
            noMatchJumpInstruction = IFLT;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }
        return new ComparisonByteCodeExpression(">=", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    static ByteCodeExpression equal(ByteCodeExpression left, ByteCodeExpression right)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        checkArgument(left.getType().equals(right.getType()), "left and right must be the same type");

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPNE;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFNE;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPL;
            noMatchJumpInstruction = IFNE;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPL;
            noMatchJumpInstruction = IFNE;
        }
        else if (type == null) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ACMPNE;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }
        return new ComparisonByteCodeExpression("==", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    static ByteCodeExpression notEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        checkArgument(left.getType().equals(right.getType()), "left and right must be the same type");

        OpCode comparisonInstruction;
        OpCode noMatchJumpInstruction;

        Class<?> type = left.getType().getPrimitiveType();
        if (type == int.class) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ICMPEQ;
        }
        else if (type == long.class) {
            comparisonInstruction = LCMP;
            noMatchJumpInstruction = IFEQ;
        }
        else if (type == float.class) {
            comparisonInstruction = FCMPL;
            noMatchJumpInstruction = IFEQ;
        }
        else if (type == double.class) {
            comparisonInstruction = DCMPL;
            noMatchJumpInstruction = IFEQ;
        }
        else if (type == null) {
            comparisonInstruction = null;
            noMatchJumpInstruction = IF_ACMPEQ;
        }
        else {
            throw new IllegalArgumentException("Less than does not support " + type);
        }
        return new ComparisonByteCodeExpression("!=", comparisonInstruction, noMatchJumpInstruction, left, right);
    }

    private static void checkArgumentTypes(ByteCodeExpression left, ByteCodeExpression right)
    {
        Class<?> leftType = getPrimitiveType(left, "left");
        Class<?> rightType = getPrimitiveType(right, "right");
        checkArgument(leftType == rightType, "left and right must be the same type");
    }

    private static Class<?> getPrimitiveType(ByteCodeExpression expression, String name)
    {
        requireNonNull(expression, name + " is null");
        Class<?> leftType = expression.getType().getPrimitiveType();
        checkArgument(leftType != null, name + " is not a primitive");
        checkArgument(leftType != void.class, name + " is void");
        return leftType;
    }

    private final String infixSymbol;
    private final OpCode comparisonInstruction;
    private final OpCode noMatchJumpInstruction;
    private final ByteCodeExpression left;
    private final ByteCodeExpression right;

    private ComparisonByteCodeExpression(
            String infixSymbol,
            OpCode comparisonInstruction,
            OpCode noMatchJumpInstruction,
            ByteCodeExpression left,
            ByteCodeExpression right)
    {
        super(type(boolean.class));
        this.infixSymbol = infixSymbol;
        this.comparisonInstruction = comparisonInstruction;
        this.noMatchJumpInstruction = noMatchJumpInstruction;
        this.left = left;
        this.right = right;
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        ByteCodeBlock block = new ByteCodeBlock()
                .append(left)
                .append(right);

        if (comparisonInstruction != null) {
            block.append(comparisonInstruction);
        }

        LabelNode noMatch = new LabelNode("no_match");
        LabelNode end = new LabelNode("end");
        return block
                .append(new JumpInstruction(noMatchJumpInstruction, noMatch))
                .push(true)
                .gotoLabel(end)
                .append(noMatch)
                .push(false)
                .append(end);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(left, right);
    }

    @Override
    protected String formatOneLine()
    {
        return "(" + left + " " + infixSymbol + " " + right + ")";
    }
}
