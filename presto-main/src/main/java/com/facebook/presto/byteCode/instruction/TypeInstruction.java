package com.facebook.presto.byteCode.instruction;

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.OpCodes;
import com.facebook.presto.byteCode.ParameterizedType;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.*;
import static com.facebook.presto.byteCode.ParameterizedType.type;

public class TypeInstruction implements InstructionNode
{
    public static InstructionNode newObject(Class<?> type)
    {
        return new TypeInstruction(NEW, type(type));
    }

    public static InstructionNode newObject(ParameterizedType type)
    {
        return new TypeInstruction(NEW, type);
    }

    public static InstructionNode newObjectArray(Class<?> type)
    {
        return new TypeInstruction(ANEWARRAY, type(type));
    }

    public static InstructionNode newObjectArray(ParameterizedType type)
    {
        return new TypeInstruction(ANEWARRAY, type);
    }

    public static InstructionNode instanceOf(Class<?> type)
    {
        return new TypeInstruction(INSTANCEOF, type(type));
    }

    public static InstructionNode instanceOf(ParameterizedType type)
    {
        return new TypeInstruction(INSTANCEOF, type);
    }

    public static InstructionNode cast(Class<?> type)
    {
        return new TypeInstruction(CHECKCAST, type(type));
    }

    public static InstructionNode cast(ParameterizedType type)
    {
        return new TypeInstruction(CHECKCAST, type);
    }

    private final OpCodes opCode;
    private final ParameterizedType type;

    public TypeInstruction(OpCodes opCode, ParameterizedType type)
    {
        this.opCode = opCode;
        this.type = type;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        visitor.visitTypeInsn(opCode.getOpCode(), type.getClassName());
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
