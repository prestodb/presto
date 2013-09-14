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

public class WhileLoop
        implements FlowControl
{
    public static WhileLoopBuilder whileLoopBuilder(CompilerContext context)
    {
        return new WhileLoopBuilder(context);
    }

    public static class WhileLoopBuilder
    {
        private final CompilerContext context;

        private final LabelNode beginLabel = new LabelNode("begin");
        private final LabelNode endLabel = new LabelNode("end");

        private String comment;
        private ByteCodeNode condition;
        private ByteCodeNode body;

        public WhileLoopBuilder(CompilerContext context)
        {
            this.context = context;
            context.pushIterationScope(beginLabel, endLabel);
        }

        public WhileLoopBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public WhileLoopBuilder condition(ByteCodeNode condition)
        {
            this.condition = buildBlock(context, condition, "condition");
            return this;
        }

        public WhileLoopBuilder condition(ByteCodeNodeFactory condition)
        {
            this.condition = buildBlock(context, condition, BOOLEAN, "condition");
            return this;
        }

        public WhileLoopBuilder body(ByteCodeNode body)
        {
            this.body = buildBlock(context, body, "body");
            return this;
        }

        public WhileLoopBuilder body(ByteCodeNodeFactory body)
        {
            this.body = buildBlock(context, body, VOID, "body");
            return this;
        }

        public WhileLoop build()
        {
            WhileLoop whileLoop = new WhileLoop(context, comment, condition, body, beginLabel, endLabel);
            context.popIterationScope();
            return whileLoop;
        }
    }

    private final CompilerContext context;
    private final String comment;
    private final ByteCodeNode condition;
    private final ByteCodeNode body;

    private final LabelNode beginLabel;
    private final LabelNode endLabel;

    private WhileLoop(CompilerContext context, String comment, ByteCodeNode condition, ByteCodeNode body, LabelNode beginLabel, LabelNode endLabel)
    {
        this.context = context;
        this.comment = comment;
        this.condition = condition;
        this.body = body;
        this.beginLabel = beginLabel;
        this.endLabel = endLabel;
    }

    public String getComment()
    {
        return comment;
    }

    public ByteCodeNode getCondition()
    {
        return condition;
    }

    public ByteCodeNode getBody()
    {
        return body;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        Block block = new Block(context)
                .visitLabel(beginLabel)
                .append(condition)
                .ifZeroGoto(endLabel)
                .append(body)
                .gotoLabel(beginLabel)
                .visitLabel(endLabel);

        block.accept(visitor);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of(condition, body);
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitWhile(parent, this);
    }
}
