package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class NamedSlot
{
    private final Optional<QualifiedName> prefix;
    private final Optional<String> attribute;
    private final Slot slot;

    public NamedSlot(Optional<QualifiedName> prefix, Optional<String> attribute, Slot slot)
    {
        Preconditions.checkNotNull(prefix, "prefix is null");
        Preconditions.checkNotNull(attribute, "attribute is null");
        Preconditions.checkNotNull(slot, "slot is null");

        this.prefix = prefix;
        this.attribute = attribute;
        this.slot = slot;
    }

    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    public Optional<String> getAttribute()
    {
        return attribute;
    }

    public Slot getSlot()
    {
        return slot;
    }

    public String toString()
    {
        return String.format("%s.%s:%s", prefix, attribute.or("<anonymous>"), slot.getType());
    }

    public static Function<NamedSlot, QualifiedName> nameGetter()
    {
        return new Function<NamedSlot, QualifiedName>()
        {
            @Override
            public QualifiedName apply(NamedSlot input)
            {
                return QualifiedName.of(input.getPrefix().get(), input.getAttribute().get());
            }
        };
    }

    public static Function<NamedSlot, Optional<String>> attributeGetter()
    {
        return new Function<NamedSlot, Optional<String>>()
        {
            @Override
            public Optional<String> apply(NamedSlot input)
            {
               return input.getAttribute();
            }
        };
    }
}
