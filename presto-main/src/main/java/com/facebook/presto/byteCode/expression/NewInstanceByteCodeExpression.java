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
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

class NewInstanceByteCodeExpression
        extends ByteCodeExpression
{
    private final List<ByteCodeExpression> parameters;
    private final ImmutableList<ParameterizedType> parameterTypes;

    public NewInstanceByteCodeExpression(
            ParameterizedType type,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        super(type);
        this.parameterTypes = ImmutableList.copyOf(checkNotNull(parameterTypes, "parameterTypes is null"));
        this.parameters = ImmutableList.copyOf(checkNotNull(parameters, "parameters is null"));
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        ByteCodeBlock block = new ByteCodeBlock()
                .newObject(getType())
                .dup();

        for (ByteCodeExpression parameter : parameters) {
            block.append(parameter);
        }
        return block.invokeConstructor(getType(), parameterTypes);
    }

    @Override
    protected String formatOneLine()
    {
        return "new " + getType().getSimpleName() + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>copyOf(parameters);
    }
}
