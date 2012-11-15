package com.facebook.presto.byteCode.instruction;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;

import java.util.List;

public class LabelNode implements InstructionNode
{
    private final String name;
    private final Label label;

    public LabelNode()
    {
        this.name = "label@" + System.identityHashCode(this);
        label = new Label();
    }

    public LabelNode(com.facebook.presto.byteCode.instruction.LabelNode labelNode)
    {
        this.name = "label@" + System.identityHashCode(this);
        label = labelNode.getLabel();
    }

    public LabelNode(String name)
    {
        this.name = name + "@" + System.identityHashCode(this);
        label = new Label();
    }

    public String getName()
    {
        return name;
    }

    public Label getLabel()
    {
        return label;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitLabel(label);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
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
        return visitor.visitLabel(parent, this);
    }
}
