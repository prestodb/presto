package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class NamedSlot
{
    private final Optional<QualifiedName> name;
    private final Slot slot;

    public NamedSlot(Optional<QualifiedName> name, Slot slot)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(slot, "slot is null");

        this.name = name;
        this.slot = slot;
    }

    public Optional<QualifiedName> getName()
    {
        return name;
    }

    public Slot getSlot()
    {
        return slot;
    }

    public String toString()
    {
        return String.format("%s:%s", name.or(QualifiedName.of("<anonymous>")), slot.getType());
    }
    
    public static Function<NamedSlot, QualifiedName> nameGetter()
    {
        return new Function<NamedSlot, QualifiedName>()
        {
            @Override
            public QualifiedName apply(NamedSlot input)
            {
                return input.getName().get();
            }
        };
    }

    public static Function<NamedSlot, Optional<QualifiedName>> optionalNameGetter()
    {
        return new Function<NamedSlot, Optional<QualifiedName>>()
        {
            @Override
            public Optional<QualifiedName> apply(NamedSlot input)
            {
                return input.getName();
            }
        };
    }

}
