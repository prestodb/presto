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
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.facebook.presto.byteCode.ByteCodeNodes.buildBlock;

public class WhileLoop
        implements FlowControl
{
    @SuppressWarnings("UnusedDeclaration")
    public static WhileLoopBuilder whileLoopBuilder()
    {
        return new WhileLoopBuilder();
    }

    public static class WhileLoopBuilder
    {
        private final LabelNode beginLabel = new LabelNode("begin");
        private final LabelNode endLabel = new LabelNode("end");

        private String comment;
        private ByteCodeNode condition;
        private ByteCodeNode body;

        public WhileLoopBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public WhileLoopBuilder condition(ByteCodeNode condition)
        {
            this.condition = buildBlock(condition, "condition");
            return this;
        }

        public WhileLoopBuilder body(ByteCodeNode body)
        {
            this.body = buildBlock(body, "body");
            return this;
        }

        public WhileLoop build()
        {
            WhileLoop whileLoop = new WhileLoop(comment, condition, body, beginLabel, endLabel);
            return whileLoop;
        }
    }

    private final String comment;
    private final ByteCodeNode condition;
    private final ByteCodeNode body;

    private final LabelNode beginLabel;
    private final LabelNode endLabel;

    private WhileLoop(String comment, ByteCodeNode condition, ByteCodeNode body, LabelNode beginLabel, LabelNode endLabel)
    {
        this.comment = comment;
        this.condition = condition;
        this.body = body;
        this.beginLabel = beginLabel;
        this.endLabel = endLabel;
    }

    @Override
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
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        Block block = new Block()
                .visitLabel(beginLabel)
                .append(condition)
                .ifZeroGoto(endLabel)
                .append(body)
                .gotoLabel(beginLabel)
                .visitLabel(endLabel);

        block.accept(visitor, generationContext);
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
