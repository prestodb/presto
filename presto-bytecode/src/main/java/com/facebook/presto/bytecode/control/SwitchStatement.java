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
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class SwitchStatement
        implements FlowControl
{
    public static SwitchBuilder switchBuilder()
    {
        return new SwitchBuilder();
    }

    private final LabelNode endLabel = new LabelNode("switchEnd");
    private final LabelNode defaultLabel = new LabelNode("switchDefault");
    private final String comment;
    private final BytecodeExpression expression;
    private final SortedSet<CaseStatement> cases;
    private final BytecodeNode defaultBody;

    private SwitchStatement(
            String comment,
            BytecodeExpression expression,
            Iterable<CaseStatement> cases,
            BytecodeNode defaultBody)
    {
        this.comment = comment;
        this.expression = requireNonNull(expression, "expression is null");
        this.cases = ImmutableSortedSet.copyOf(comparing(CaseStatement::getKey), cases);
        this.defaultBody = defaultBody;
    }

    @Override
    public String getComment()
    {
        return comment;
    }

    public BytecodeExpression expression()
    {
        return expression;
    }

    public SortedSet<CaseStatement> cases()
    {
        return cases;
    }

    public LabelNode getDefaultLabel()
    {
        return defaultLabel;
    }

    public BytecodeNode getDefaultBody()
    {
        return defaultBody;
    }

    public LabelNode getEndLabel()
    {
        return endLabel;
    }

    @Override
    public void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        // build switch table
        int[] keys = new int[cases.size()];
        Label[] labels = new Label[cases.size()];

        int index = 0;
        for (CaseStatement caseStatement : cases) {
            keys[index] = caseStatement.getKey();
            labels[index] = caseStatement.getLabel().getLabel();
            index++;
        }

        // build case blocks
        BytecodeBlock block = new BytecodeBlock();

        for (CaseStatement caseStatement : cases) {
            block.visitLabel(caseStatement.getLabel())
                    .append(caseStatement.getBody())
                    .gotoLabel(endLabel);
        }

        // build default block
        block.visitLabel(defaultLabel);

        if (defaultBody != null) {
            block.append(defaultBody);
        }

        block.visitLabel(endLabel);

        // emit code
        expression.accept(visitor, generationContext);
        visitor.visitLookupSwitchInsn(defaultLabel.getLabel(), keys, labels);
        block.accept(visitor, generationContext);
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitSwitch(parent, this);
    }

    public static class SwitchBuilder
    {
        private final Set<CaseStatement> cases = new HashSet<>();
        private String comment;
        private BytecodeExpression expression;
        private LabelNode defaultLabel;
        private BytecodeNode defaultBody;

        public SwitchBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public SwitchBuilder expression(BytecodeExpression expression)
        {
            this.expression = expression;
            return this;
        }

        public SwitchBuilder addCase(int key, BytecodeNode body)
        {
            LabelNode label = new LabelNode("switchCase:" + key);
            CaseStatement statement = new CaseStatement(key, body, label);
            checkState(cases.add(statement), "case already exists for value [%s]", key);
            return this;
        }

        public SwitchBuilder defaultCase(BytecodeNode body)
        {
            checkState(defaultBody == null, "default case already set");
            this.defaultBody = requireNonNull(body, "body is null");
            return this;
        }

        public SwitchStatement build()
        {
            checkState(expression != null, "expression is not set");
            return new SwitchStatement(comment, expression, cases, defaultBody);
        }
    }
}
