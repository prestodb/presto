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
package com.facebook.presto.byteCode.instruction;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BasicInstruction
        implements InstructionNode
{
    protected final int opCode;

    public BasicInstruction(int opCode)
    {
        this.opCode = opCode;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitInsn(opCode);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("opCode", opCode)
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
        return visitor.visitInstruction(parent, this);
    }
}
