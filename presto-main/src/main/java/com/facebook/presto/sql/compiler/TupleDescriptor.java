package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

import static com.facebook.presto.sql.compiler.NamedSlot.optionalNameGetter;

public class TupleDescriptor
{
    private final List<NamedSlot> slots;

    public TupleDescriptor(List<NamedSlot> slots)
    {
        Preconditions.checkNotNull(slots, "slots is null");
        this.slots = ImmutableList.copyOf(slots);
    }

    public TupleDescriptor(List<Optional<QualifiedName>> names, List<Slot> slots)
    {
        Preconditions.checkNotNull(names, "names is null");
        Preconditions.checkNotNull(slots, "slots is null");
        Preconditions.checkArgument(names.size() == slots.size(), "names and slots sizes do not match");

        ImmutableList.Builder<NamedSlot> builder = ImmutableList.builder();
        for (int i = 0; i < names.size(); i++) {
            builder.add(new NamedSlot(names.get(i), slots.get(i)));
        }

        this.slots = builder.build();
    }

    public List<Optional<QualifiedName>> getNames()
    {
        return Lists.transform(slots, optionalNameGetter());
    }

    public List<NamedSlot> getSlots()
    {
        return slots;
    }

    @Override
    public String toString()
    {
        return Joiner.on(",").join(slots);
    }

    public List<NamedSlot> resolve(final QualifiedName name)
    {
        return ImmutableList.copyOf(Iterables.filter(slots, new Predicate<NamedSlot>()
        {
            @Override
            public boolean apply(NamedSlot input)
            {
                return input.getName().isPresent() && input.getName().get().hasSuffix(name);
            }
        }));
    }
}
