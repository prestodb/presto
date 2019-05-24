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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class InlineIfBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression condition;
    private final BytecodeExpression ifTrue;
    private final BytecodeExpression ifFalse;

    InlineIfBytecodeExpression(BytecodeExpression condition, BytecodeExpression ifTrue, BytecodeExpression ifFalse)
    {
        super(ifTrue.getType());
        this.condition = condition;
        this.ifTrue = requireNonNull(ifTrue, "ifTrue is null");
        this.ifFalse = requireNonNull(ifFalse, "ifFalse is null");

        checkArgument(condition.getType().getPrimitiveType() == boolean.class, "Expected condition to be type boolean but is %s", condition.getType());
        checkArgument(ifTrue.getType().equals(ifFalse.getType()), "Expected ifTrue and ifFalse to be the same type");
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        LabelNode falseLabel = new LabelNode("false");
        LabelNode endLabel = new LabelNode("end");
        return new BytecodeBlock()
                .append(condition)
                .ifFalseGoto(falseLabel)
                .append(ifTrue)
                .gotoLabel(endLabel)
                .visitLabel(falseLabel)
                .append(ifFalse)
                .visitLabel(endLabel);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of(condition, ifTrue, ifFalse);
    }

    @Override
    protected String formatOneLine()
    {
        return "(" + condition + " ? " + ifTrue + " : " + ifFalse + ")";
    }
}
