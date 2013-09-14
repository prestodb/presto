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
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.ILOAD;
import static com.facebook.presto.byteCode.OpCodes.ISTORE;

public abstract class VariableInstruction
        implements InstructionNode
{
    public static InstructionNode loadVariable(int slot)
    {
        return new LoadVariableInstruction(new LocalVariableDefinition("unknown", slot, ParameterizedType.type(Object.class)));
    }

    public static InstructionNode loadVariable(LocalVariableDefinition variable)
    {
        return new LoadVariableInstruction(variable);
    }

    public static InstructionNode storeVariable(LocalVariableDefinition variable)
    {
        return new StoreVariableInstruction(variable);
    }

    public static InstructionNode incrementVariable(LocalVariableDefinition variable, byte increment)
    {
        return new IncrementVariableInstruction(variable, increment);
    }

    private final LocalVariableDefinition variable;

    private VariableInstruction(LocalVariableDefinition variable)
    {
        this.variable = variable;
    }

    public LocalVariableDefinition getVariable()
    {
        return variable;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitVariableInstruction(parent, this);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("variable", variable)
                .toString();
    }

    public static class LoadVariableInstruction
            extends VariableInstruction
    {
        public LoadVariableInstruction(LocalVariableDefinition variable)
        {
            super(variable);
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            visitor.visitVarInsn(Type.getType(getVariable().getType().getType()).getOpcode(ILOAD.getOpCode()), getVariable().getSlot());
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitLoadVariable(parent, this);
        }
    }

    public static class StoreVariableInstruction
            extends VariableInstruction
    {
        public StoreVariableInstruction(LocalVariableDefinition variable)
        {
            super(variable);
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            visitor.visitVarInsn(Type.getType(getVariable().getType().getType()).getOpcode(ISTORE.getOpCode()), getVariable().getSlot());
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitStoreVariable(parent, this);
        }
    }

    public static class IncrementVariableInstruction
            extends VariableInstruction
    {
        private final byte increment;

        public IncrementVariableInstruction(LocalVariableDefinition variable, byte increment)
        {
            super(variable);
            String type = variable.getType().getClassName();
            Preconditions.checkArgument(ImmutableList.of("byte", "short", "int").contains(type), "variable must be an byte, short or int, but is %s", type);
            this.increment = increment;
        }

        public byte getIncrement()
        {
            return increment;
        }

        @Override
        public void accept(MethodVisitor visitor)
        {
            visitor.visitIincInsn(getVariable().getSlot(), increment);
        }

        @Override
        public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
        {
            return visitor.visitIncrementVariable(parent, this);
        }
    }
}
