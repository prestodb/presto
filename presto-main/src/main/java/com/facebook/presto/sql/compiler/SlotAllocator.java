package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;

public class SlotAllocator
{
    private int slotId;

    public Slot newSlot(Type type)
    {
        Preconditions.checkNotNull(type, "type is null");

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
