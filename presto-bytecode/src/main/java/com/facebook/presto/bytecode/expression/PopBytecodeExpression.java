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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

class PopBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression instance;

    PopBytecodeExpression(BytecodeExpression instance)
    {
        super(type(void.class));
        this.instance = requireNonNull(instance, "instance is null");
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return new BytecodeBlock()
                .append(instance.getBytecode(generationContext))
                .pop(instance.getType());
    }

    @Override
    protected String formatOneLine()
    {
        return instance.toString();
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(instance);
    }
}
