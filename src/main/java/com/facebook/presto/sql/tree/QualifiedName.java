package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class QualifiedName
{
    private final List<String> parts;

    public QualifiedName(String name)
    {
        this(ImmutableList.of(name));
    }

    public QualifiedName(List<String> parts)
    {
        checkNotNull(parts, "parts");
        checkArgument(!parts.isEmpty(), "parts is empty");
        this.parts = ImmutableList.copyOf(parts);
    }

    public List<String> getParts()
    {
        return parts;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(parts)
                .toString();
    }
}
