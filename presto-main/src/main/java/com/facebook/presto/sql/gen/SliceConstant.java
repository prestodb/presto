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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.byteCode.instruction.InstructionNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.objectweb.asm.MethodVisitor;

import java.lang.invoke.MethodType;

import static com.facebook.presto.byteCode.instruction.InvokeInstruction.invokeDynamic;
import static com.facebook.presto.sql.gen.SliceLiteralBootstrap.SLICE_LITERAL_BOOTSTRAP;
import static com.google.common.base.Charsets.UTF_8;

public class SliceConstant
        extends Constant
{
    public static SliceConstant sliceConstant(String value)
    {
        return new SliceConstant(Slices.copiedBuffer(value, UTF_8));
    }

    public static SliceConstant sliceConstant(Slice value)
    {
        return new SliceConstant(value);
    }

    private final Slice value;

    private SliceConstant(Slice value)
    {
        this.value = value;
    }

    @Override
    public Slice getValue()
    {
        return value;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        InstructionNode node = invokeDynamic("load", MethodType.methodType(Slice.class), SLICE_LITERAL_BOOTSTRAP, value.toString(UTF_8));
        node.accept(visitor);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitConstant(parent, this);
    }

    @Override
    public String toString()
    {
        return "load constant '" + value.toString(UTF_8) + "'";
    }
}
