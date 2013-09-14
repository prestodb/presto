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
import com.facebook.presto.byteCode.ByteCodeNodeFactory;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.ByteCodeNodes.buildBlock;
import static com.facebook.presto.byteCode.ExpectedType.BOOLEAN;
import static com.facebook.presto.byteCode.ExpectedType.VOID;

public class DoWhileLoop
        implements FlowControl
{
    public static DoWhileLoopBuilder doWhileLoopBuilder(CompilerContext context)
    {
        return new DoWhileLoopBuilder(context);
    }

    public static class DoWhileLoopBuilder
    {
        private final CompilerContext context;

        private final LabelNode continueLabel = new LabelNode("continue");
        private final LabelNode endLabel = new LabelNode("end");

        private String comment;
        private Block body;
        private Block condition;

        public DoWhileLoopBuilder(CompilerContext context)
        {
            this.context = context;
            context.pushIterationScope(continueLabel, endLabel);
        }

        public DoWhileLoopBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public DoWhileLoopBuilder body(ByteCodeNodeFactory body)
        {
            this.body = buildBlock(context, body, VOID, "body");
            return this;
        }

        public DoWhileLoopBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public DoWhileLoop build()
        {
            DoWhileLoop doWhileLoop = new DoWhileLoop(context, comment, body, condition, continueLabel, endLabel);
            context.popIterationScope();
            return doWhileLoop;
        }
    }

    private final CompilerContext context;
    private final String comment;
    private final Block body;
    private final Block condition;

    private final LabelNode beginLabel = new LabelNode("begin");
    private final LabelNode continueLabel;
    private final LabelNode endLabel;

    private DoWhileLoop(CompilerContext context, String comment, Block body, Block condition, LabelNode continueLabel, LabelNode endLabel)
    {
        this.context = context;
        this.comment = comment;
        this.body = body;
        this.condition = condition;

        this.continueLabel = continueLabel;
        this.endLabel = endLabel;
    }

    public String getComment()
    {
        return comment;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        Block block = new Block(context)
                .visitLabel(beginLabel)
                .append(body)
                .visitLabel(continueLabel)
                .append(condition)
                .ifZeroGoto(endLabel)
                .gotoLabel(beginLabel)
                .visitLabel(endLabel);

        block.accept(visitor);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(body, condition);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitDoWhile(parent, this);
    }
}
