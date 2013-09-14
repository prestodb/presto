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
package com.facebook.presto.byteCode.debug;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

public class LocalVariableNode
        implements DebugNode
{
    private final LocalVariableDefinition variable;
    private final LabelNode start;
    private final LabelNode end;

    public LocalVariableNode(LocalVariableDefinition variable, LabelNode start, LabelNode end)
    {
        this.variable = variable;
        this.start = start;
        this.end = end;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitLocalVariable(variable.getName(),
                variable.getType().getType(),
                variable.getType().getGenericSignature(),
                start.getLabel(),
                end.getLabel(),
                variable.getSlot());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("variable", variable)
                .add("start", start)
                .add("end", end)
                .toString();
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitLocalVariable(parent, this);
    }
}
