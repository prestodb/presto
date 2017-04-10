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
package com.facebook.presto.bytecode.control;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TryCatch
        implements FlowControl
{
    private final String comment;
    private final BytecodeNode tryNode;
    private final BytecodeNode catchNode;
    private final String exceptionName;

    public TryCatch(BytecodeNode tryNode, BytecodeNode catchNode, ParameterizedType exceptionType)
    {
        this(null, tryNode, catchNode, exceptionType);
    }

    public TryCatch(String comment, BytecodeNode tryNode, BytecodeNode catchNode, ParameterizedType exceptionType)
    {
        this.comment = comment;
        this.tryNode = requireNonNull(tryNode, "tryNode is null");
        this.catchNode = requireNonNull(catchNode, "catchNode is null");
        this.exceptionName = (exceptionType != null) ? exceptionType.getClassName() : null;
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public BytecodeNode getTryNode()
    {
        return tryNode;
    }

    public BytecodeNode getCatchNode()
    {
        return catchNode;
    }

    public String getExceptionName()
    {
        return exceptionName;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        LabelNode tryStart = new LabelNode("tryStart");
        LabelNode tryEnd = new LabelNode("tryEnd");
        LabelNode handler = new LabelNode("handler");
        LabelNode done = new LabelNode("done");

        BytecodeBlock block = new BytecodeBlock();

        // try block
        block.visitLabel(tryStart)
                .append(tryNode)
                .visitLabel(tryEnd)
                .gotoLabel(done);

        // handler block
        block.visitLabel(handler)
                .append(catchNode);

        // all done
        block.visitLabel(done);

        block.accept(visitor, generationContext);
        visitor.visitTryCatchBlock(tryStart.getLabel(), tryEnd.getLabel(), handler.getLabel(), exceptionName);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(tryNode, catchNode);
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitTryCatch(parent, this);
    }
}
