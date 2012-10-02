package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class QualifiedName
{
    private final List<String> parts;

    public static QualifiedName of(String first, String... rest)
    {
        Preconditions.checkNotNull(first, "first is null");
        return new QualifiedName(ImmutableList.copyOf(Lists.asList(first, rest)));
    }

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

    public static Predicate<? super QualifiedName> hasSuffix(final QualifiedName suffix)
    {
        return new Predicate<QualifiedName>()
        {
            @Override
            public boolean apply(QualifiedName name)
            {
                if (name.getParts().size() < suffix.getParts().size()) {
                    return false;
                }

                int start = name.getParts().size() - suffix.getParts().size();

                return name.getParts().subList(start, name.getParts().size()).equals(suffix.getParts());
            }
        };
    }
}
