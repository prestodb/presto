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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.OpCode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.expression.ArithmeticByteCodeExpression.getNumericOpCode;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class NegateByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression value;
    private final OpCode negateOpCode;

    NegateByteCodeExpression(ByteCodeExpression value)
    {
        super(checkNotNull(value, "value is null").getType());
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
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new Block()
                .append(value)
                .append(negateOpCode);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(value);
    }

    @Override
    protected String formatOneLine()
    {
        return "-(" + value + ")";
    }
}
