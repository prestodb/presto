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
import com.facebook.presto.byteCode.OpCodes;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.OpCodes.ANEWARRAY;
import static com.facebook.presto.byteCode.OpCodes.CHECKCAST;
import static com.facebook.presto.byteCode.OpCodes.INSTANCEOF;
import static com.facebook.presto.byteCode.OpCodes.NEW;
import static com.facebook.presto.byteCode.ParameterizedType.type;

public class TypeInstruction
        implements InstructionNode
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
