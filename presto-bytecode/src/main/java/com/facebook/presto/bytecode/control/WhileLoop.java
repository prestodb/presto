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
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class WhileLoop
        implements FlowControl
{
    private final String comment;
    private final BytecodeBlock condition = new BytecodeBlock();
    private final BytecodeBlock body = new BytecodeBlock();

    private final LabelNode continueLabel = new LabelNode("continue");
    private final LabelNode endLabel = new LabelNode("end");

    public WhileLoop()
    {
        this.comment = null;
    }

    public WhileLoop(String format, Object... args)
    {
        this.comment = String.format(format, args);
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public LabelNode getContinueLabel()
    {
        return continueLabel;
    }

    public LabelNode getEndLabel()
    {
        return endLabel;
    }

    public BytecodeBlock condition()
    {
        return condition;
    }

    public WhileLoop condition(BytecodeNode node)
    {
        checkState(condition.isEmpty(), "condition already set");
        condition.append(node);
        return this;
    }

    public BytecodeBlock body()
    {
        return body;
    }

    public WhileLoop body(BytecodeNode node)
    {
        checkState(body.isEmpty(), "body already set");
        body.append(node);
        return this;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        checkState(!condition.isEmpty(), "WhileLoop does not have a condition set");

        BytecodeBlock block = new BytecodeBlock()
                .visitLabel(continueLabel)
                .append(condition)
                .ifZeroGoto(endLabel)
                .append(body)
                .gotoLabel(continueLabel)
                .visitLabel(endLabel);

        block.accept(visitor, generationContext);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(condition, body);
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitWhile(parent, this);
    }
}
