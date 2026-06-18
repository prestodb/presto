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
import com.facebook.presto.bytecode.instruction.InstructionNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.bytecode.ArrayOpCode.getArrayOpCode;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class GetElementBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression instance;
    private final BytecodeExpression index;
    private final InstructionNode arrayLoadInstruction;

    public GetElementBytecodeExpression(BytecodeExpression instance, BytecodeExpression index)
    {
        super(instance.getType().getArrayComponentType());
        this.instance = requireNonNull(instance, "instance is null");
        this.index = requireNonNull(index, "index is null");

        checkArgument(index.getType().getPrimitiveType() == int.class, "index must be int type, but is " + index.getType());
        this.arrayLoadInstruction = getArrayOpCode(instance.getType().getArrayComponentType()).getLoad();
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return new BytecodeBlock()
                .append(instance.getBytecode(generationContext))
                .append(index)
                .append(arrayLoadInstruction);
    }

    @Override
    protected String formatOneLine()
    {
        return instance + "[" + index + "]";
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(index);
    }
}
