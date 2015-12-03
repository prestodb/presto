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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.byteCode.OpCode.ARRAYLENGTH;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class ArrayLengthByteCodeExpression
    extends ByteCodeExpression
{
    private final ByteCodeExpression instance;

    public ArrayLengthByteCodeExpression(ByteCodeExpression instance)
    {
        super(type(int.class));
        this.instance = requireNonNull(instance, "instance is null");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext))
                .append(ARRAYLENGTH);
    }

    @Override
    protected String formatOneLine()
    {
        return instance + ".length";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of();
    }
}
