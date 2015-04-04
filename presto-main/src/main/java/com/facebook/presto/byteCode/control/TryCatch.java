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
package com.facebook.presto.byteCode.control;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class TryCatch
        implements FlowControl
{
    private final String comment;
    private final ByteCodeNode tryNode;
    private final ByteCodeNode catchNode;
    private final String exceptionName;

    public TryCatch(String comment, ByteCodeNode tryNode, ByteCodeNode catchNode, ParameterizedType exceptionType)
    {
        this.comment = comment;
        this.tryNode = checkNotNull(tryNode, "tryNode is null");
        this.catchNode = checkNotNull(catchNode, "catchNode is null");
        this.exceptionName = (exceptionType != null) ? exceptionType.getClassName() : null;
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public ByteCodeNode getTryNode()
    {
        return tryNode;
    }

    public ByteCodeNode getCatchNode()
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

        Block block = new Block();

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
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of(tryNode, catchNode);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitTryCatch(parent, this);
    }
}
