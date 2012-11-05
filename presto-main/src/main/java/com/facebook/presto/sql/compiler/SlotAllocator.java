package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Expression;

public class SlotAllocator
{
    private int slotId;

    public Slot newSlot(Type type)
    {
        return new Slot(slotId++, type);
    }

    public Slot newSlot(AnalyzedExpression expression)
    {
        return newSlot(expression.getType(), expression.getRewrittenExpression());
    }

    public Slot newSlot(Type type, Expression expression)
    {
        if (expression instanceof SlotReference) {
            return ((SlotReference) expression).getSlot();
        }

        return newSlot(type);
    }
}
