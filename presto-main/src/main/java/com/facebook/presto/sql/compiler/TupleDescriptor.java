package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.sql.compiler.NamedSlot.attributeGetter;

public class TupleDescriptor
{
    private final List<NamedSlot> slots;

    public TupleDescriptor(List<NamedSlot> slots)
    {
        Preconditions.checkNotNull(slots, "slots is null");
        this.slots = ImmutableList.copyOf(slots);
    }

    public TupleDescriptor(List<Optional<String>> attributes, List<Slot> slots)
    {
        Preconditions.checkNotNull(attributes, "attributes is null");
        Preconditions.checkNotNull(slots, "slots is null");
        Preconditions.checkArgument(attributes.size() == slots.size(), "attributes and slots sizes do not match");

        ImmutableList.Builder<NamedSlot> builder = ImmutableList.builder();
        for (int i = 0; i < attributes.size(); i++) {
            builder.add(new NamedSlot(Optional.<QualifiedName>absent(), attributes.get(i), slots.get(i)));
        }

        this.slots = builder.build();
    }

    public List<Optional<String>> getAttributes()
    {
        return Lists.transform(slots, attributeGetter());
    }

    public List<NamedSlot> getSlots()
    {
        return slots;
    }

    @Override
    public String toString()
    {
        return slots.toString();
    }

    public List<NamedSlot> resolve(final QualifiedName name)
    {
        return ImmutableList.copyOf(Iterables.filter(slots, new Predicate<NamedSlot>()
        {
            @Override
            public boolean apply(NamedSlot input)
            {
                if (!input.getPrefix().isPresent() || !input.getAttribute().isPresent()) {
                    return false;
                }

                return QualifiedName.of(input.getPrefix().get(), input.getAttribute().get()).hasSuffix(name);
            }
        }));
    }
}
