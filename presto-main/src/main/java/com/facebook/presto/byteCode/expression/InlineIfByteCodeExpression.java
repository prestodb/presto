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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class InlineIfByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression condition;
    private final ByteCodeExpression ifTrue;
    private final ByteCodeExpression ifFalse;

    InlineIfByteCodeExpression(ByteCodeExpression condition, ByteCodeExpression ifTrue, ByteCodeExpression ifFalse)
    {
        super(ifTrue.getType());
        this.condition = condition;
        this.ifTrue = checkNotNull(ifTrue, "ifTrue is null");
        this.ifFalse = checkNotNull(ifFalse, "ifFalse is null");

        checkArgument(condition.getType().getPrimitiveType() == boolean.class, "Expected condition to be type boolean but is %s", condition.getType());
        checkArgument(ifTrue.getType().equals(ifFalse.getType()), "Expected ifTrue and ifFalse to be the same type");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        LabelNode falseLabel = new LabelNode("false");
        LabelNode endLabel = new LabelNode("end");
        return new ByteCodeBlock()
                .append(condition)
                .ifFalseGoto(falseLabel)
                .append(ifTrue)
                .gotoLabel(endLabel)
                .visitLabel(falseLabel)
                .append(ifFalse)
                .visitLabel(endLabel);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.<ByteCodeNode>of(condition, ifTrue, ifFalse);
    }

    @Override
    protected String formatOneLine()
    {
        return "(" + condition + " ? " + ifTrue + " : " + ifFalse + ")";
    }
}
