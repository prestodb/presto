package com.facebook.presto.byteCode.instruction;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;

import java.util.List;

public class BasicInstruction implements InstructionNode
{
    protected final int opCode;

    public BasicInstruction(int opCode)
    {
        this.opCode = opCode;
    }

    public void accept(MethodVisitor visitor)
    {
        visitor.visitInsn(opCode);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
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
