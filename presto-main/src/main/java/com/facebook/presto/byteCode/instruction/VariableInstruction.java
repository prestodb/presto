package com.facebook.presto.byteCode.instruction;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.ParameterizedType;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.ILOAD;
import static com.facebook.presto.byteCode.OpCodes.ISTORE;

public abstract class VariableInstruction implements InstructionNode
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

    public static class LoadVariableInstruction extends VariableInstruction
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

    public static class StoreVariableInstruction extends VariableInstruction
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
}
