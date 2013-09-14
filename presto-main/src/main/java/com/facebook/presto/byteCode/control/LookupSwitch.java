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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

public class LookupSwitch
        implements FlowControl
{
    public static LookupSwitchBuilder lookupSwitchBuilder()
    {
        return new LookupSwitchBuilder();
    }

    public static class LookupSwitchBuilder
    {
        private final List<CaseStatement> cases = new ArrayList<>();
        private String comment;
        private LabelNode defaultCase;

        public LookupSwitchBuilder defaultCase(LabelNode defaultCase)
        {
            this.defaultCase = defaultCase;
            return this;
        }

        public LookupSwitchBuilder comment(String format, Object... args)
        {
            this.comment = String.format(format, args);
            return this;
        }

        public LookupSwitchBuilder addCase(int key, LabelNode label)
        {
            cases.add(CaseStatement.caseStatement(key, label));
            return this;
        }

        public LookupSwitchBuilder addCase(CaseStatement caseStatement)
        {
            cases.add(caseStatement);
            return this;
        }

        public LookupSwitch build()
        {
            return new LookupSwitch(comment, defaultCase, cases);
        }
    }

    private final String comment;
    private final LabelNode defaultCase;
    private final SortedSet<CaseStatement> cases;

    private LookupSwitch(String comment, LabelNode defaultCase, Iterable<CaseStatement> cases)
    {
        this.comment = comment;
        this.defaultCase = defaultCase;
        this.cases = ImmutableSortedSet.copyOf(cases);
    }

    public String getComment()
    {
        return comment;
    }

    public SortedSet<CaseStatement> getCases()
    {
        return cases;
    }

    public LabelNode getDefaultCase()
    {
        return defaultCase;
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        int[] keys = new int[cases.size()];
        Label[] labels = new Label[cases.size()];

        int index = 0;
        for (CaseStatement caseStatement : cases) {
            keys[index] = caseStatement.getKey();
            labels[index] = caseStatement.getLabel().getLabel();
            index++;
        }
        visitor.visitLookupSwitchInsn(defaultCase.getLabel(), keys, labels);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    public <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitLookupSwitch(parent, this);
    }
}
