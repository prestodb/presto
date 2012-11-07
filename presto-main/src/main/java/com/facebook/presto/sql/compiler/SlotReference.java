package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;

import static java.lang.String.format;

public class SlotReference
    extends Expression
{
    private final Slot slot;

    public SlotReference(Slot slot)
    {
        Preconditions.checkNotNull(slot, "slot is null");
        this.slot = slot;
    }

    public Slot getSlot()
    {
        return slot;
    }

    @Override
    public String toString()
    {
        return format("Slot[%s]", slot);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSlotReference(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SlotReference that = (SlotReference) o;

        if (!slot.equals(that.slot)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return slot.hashCode();
    }
}
