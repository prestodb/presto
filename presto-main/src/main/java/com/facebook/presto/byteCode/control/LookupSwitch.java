package com.facebook.presto.byteCode.control;

import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.instruction.LabelNode;

import java.util.ArrayList;
import java.util.List;

public class LookupSwitch implements FlowControl
{
    public static LookupSwitchBuilder lookupSwitchBuilder()
    {
        return new LookupSwitchBuilder();
    }

    public static class LookupSwitchBuilder
    {
        private final List<CaseStatement> cases = new ArrayList<>();
        private LabelNode defaultCase;

        public LookupSwitchBuilder defaultCase(LabelNode defaultCase)
        {
            this.defaultCase = defaultCase;
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
            return new LookupSwitch(defaultCase, cases);
        }
    }

    private final LabelNode defaultCase;
    private final List<CaseStatement> cases;

    private LookupSwitch(LabelNode defaultCase, Iterable<CaseStatement> cases)
    {
        this.defaultCase = defaultCase;
        this.cases = ImmutableList.copyOf(cases);
    }

    @Override
    public void accept(MethodVisitor visitor)
    {
        int[] keys = new int[cases.size()];
        Label[] labels = new Label[cases.size()];
        for (int i = 0; i < cases.size(); i++) {
            keys[i] = cases.get(i).getKey();
            labels[i] = cases.get(i).getLabel().getLabel();
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
