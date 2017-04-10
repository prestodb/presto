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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArithmeticBytecodeExpression
        extends BytecodeExpression
{
    public static BytecodeExpression createArithmeticBytecodeExpression(OpCode baseOpCode, BytecodeExpression left, BytecodeExpression right)
    {
        requireNonNull(baseOpCode, "baseOpCode is null");
        String name = getName(baseOpCode);
        String infixSymbol = getInfixSymbol(baseOpCode);

        checkArgumentTypes(baseOpCode, name, left, right);

        OpCode opCode = getNumericOpCode(name, baseOpCode, left.getType().getPrimitiveType());
        return new ArithmeticBytecodeExpression(infixSymbol, left.getType(), opCode, left, right);
    }

    private static String getName(OpCode baseOpCode)
    {
        switch (baseOpCode) {
            case IAND:
                return "Bitwise AND";
            case IOR:
                return "Bitwise OR";
            case IXOR:
                return "Bitwise XOR";
            case IADD:
                return "Add";
            case ISUB:
                return "Subtract";
            case IMUL:
                return "Multiply";
            case IDIV:
                return "Divide";
            case IREM:
                return "Remainder";
            case ISHL:
                return "Shift left";
            case ISHR:
                return "Shift right";
            case IUSHR:
                return "Shift right unsigned";
            default:
                throw new IllegalArgumentException("Unsupported OpCode " + baseOpCode);
        }
    }

    private static String getInfixSymbol(OpCode baseOpCode)
    {
        switch (baseOpCode) {
            case IAND:
                return "&";
            case IOR:
                return "|";
            case IXOR:
                return "^";
            case IADD:
                return "+";
            case ISUB:
                return "-";
            case IMUL:
                return "*";
            case IDIV:
                return "/";
            case IREM:
                return "%";
            case ISHL:
                return "<<";
            case ISHR:
                return ">>";
            case IUSHR:
                return ">>>";
            default:
                throw new IllegalArgumentException("Unsupported OpCode " + baseOpCode);
        }
    }

    private static void checkArgumentTypes(OpCode baseOpCode, String name, BytecodeExpression left, BytecodeExpression right)
    {
        Class<?> leftType = getPrimitiveType(left, "left");
        Class<?> rightType = getPrimitiveType(right, "right");
        switch (baseOpCode) {
            case IAND:
            case IOR:
            case IXOR:
                checkArgument(leftType == rightType, "left and right must be the same type");
                checkArgument(leftType == int.class || leftType == long.class, "%s argument must be int or long, but is %s", name, leftType);
                return;
            case IADD:
            case ISUB:
            case IMUL:
            case IDIV:
            case IREM:
                checkArgument(leftType == rightType, "left and right must be the same type");
                checkArgument(leftType == int.class || leftType == long.class || leftType == float.class || leftType == double.class,
                        "%s argument must be int, long, float, or double, but is %s",
                        name,
                        leftType);
                return;
            case ISHL:
            case ISHR:
            case IUSHR:
                checkArgument(leftType == int.class || leftType == long.class, "%s left argument be int or long, but is %s", name, leftType);
                checkArgument(rightType == int.class, "%s right argument be and int, but is %s", name, rightType);
                return;
            default:
                throw new IllegalArgumentException("Unsupported OpCode " + baseOpCode);
        }
    }

    static OpCode getNumericOpCode(String name, OpCode baseOpCode, Class<?> type)
    {
        // Arithmetic OpCodes are laid out int, long, float and then double
        if (type == int.class) {
            return baseOpCode;
        }
        else if (type == long.class) {
            return OpCode.getOpCode(baseOpCode.getOpCode() + 1);
        }
        else if (type == float.class) {
            return OpCode.getOpCode(baseOpCode.getOpCode() + 2);
        }
        else if (type == double.class) {
            return OpCode.getOpCode(baseOpCode.getOpCode() + 3);
        }
        else {
            throw new IllegalArgumentException(name + " does not support " + type);
        }
    }

    private static Class<?> getPrimitiveType(BytecodeExpression expression, String name)
    {
        requireNonNull(expression, name + " is null");
        Class<?> leftType = expression.getType().getPrimitiveType();
        checkArgument(leftType != null, name + " is not a primitive");
        checkArgument(leftType != void.class, name + " is void");
        return leftType;
    }

    private final String infixSymbol;
    private final OpCode opCode;
    private final BytecodeExpression left;
    private final BytecodeExpression right;

    private ArithmeticBytecodeExpression(
            String infixSymbol,
            ParameterizedType type,
            OpCode opCode,
            BytecodeExpression left,
            BytecodeExpression right)
    {
        super(type);
        this.infixSymbol = infixSymbol;
        this.opCode = opCode;
        this.left = left;
        this.right = right;
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return new BytecodeBlock()
                .append(left)
                .append(right)
                .append(opCode);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    protected String formatOneLine()
    {
        return "(" + left + " " + infixSymbol + " " + right + ")";
    }
}
