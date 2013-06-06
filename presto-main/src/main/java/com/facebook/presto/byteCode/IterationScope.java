package com.facebook.presto.byteCode;


import com.facebook.presto.byteCode.instruction.LabelNode;

public class IterationScope
{
    private final LabelNode begin;
    private final LabelNode end;

    public IterationScope(LabelNode begin, LabelNode end)
    {
        this.begin = begin;
        this.end = end;
    }

    public LabelNode getBegin()
    {
        return begin;
    }

    public LabelNode getEnd()
    {
        return end;
    }
}
