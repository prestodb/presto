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

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class InstanceOfByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final Class<?> type;

    public InstanceOfByteCodeExpression(ByteCodeExpression instance, Class<?> type)
    {
        super(type(boolean.class));

        this.instance = requireNonNull(instance, "instance is null");
        this.type = requireNonNull(type, "type is null");
    }

    public static ByteCodeExpression instanceOf(ByteCodeExpression instance, Class<?> type)
    {
        return new InstanceOfByteCodeExpression(instance, type);
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return new ByteCodeBlock()
                .append(instance)
                .isInstanceOf(type);
    }

    @Override
    protected String formatOneLine()
    {
        return instance + " instanceof " + type;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }
}
