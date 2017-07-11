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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.bytecode.expression.ArithmeticBytecodeExpression.getNumericOpCode;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class NegateBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression value;
    private final OpCode negateOpCode;

    NegateBytecodeExpression(BytecodeExpression value)
    {
        super(requireNonNull(value, "value is null").getType());
        this.value = value;

        Class<?> type = value.getType().getPrimitiveType();
        checkArgument(type != null, "value is not a primitive");
        checkArgument(type != void.class, "value is void");
        checkArgument(type == int.class || type == long.class || type == float.class || type == double.class,
                "value argument must be int, long, float, or double, but is %s",
                type);

        negateOpCode = getNumericOpCode("Negate", OpCode.INEG, type);
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return new BytecodeBlock()
                .append(value)
                .append(negateOpCode);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(value);
    }

    @Override
    protected String formatOneLine()
    {
        return "-(" + value + ")";
    }
}
