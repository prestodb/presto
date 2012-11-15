package com.facebook.presto.byteCode.debug;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.LocalVariableDefinition;
import com.facebook.presto.byteCode.instruction.LabelNode;

import java.util.List;

public class LocalVariableNode implements DebugNode
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
