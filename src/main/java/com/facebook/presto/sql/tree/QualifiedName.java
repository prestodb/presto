package com.facebook.presto.sql.tree;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
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

    public static QualifiedName of(Iterable<String> parts)
    {
        Preconditions.checkNotNull(parts, "parts is null");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        return new QualifiedName(parts);
    }

    public QualifiedName(String name)
    {
        this(ImmutableList.of(name));
    }

    public QualifiedName(Iterable<String> parts)
    {
        Preconditions.checkNotNull(parts, "parts");
        Preconditions.checkArgument(!Iterables.isEmpty(parts), "parts is empty");

        this.parts = ImmutableList.copyOf(parts);
    }

    public List<String> getParts()
    {
        return parts;
    }

    @Override
    public String toString()
    {
        return Joiner.on('.').join(parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c"
     * For an identifier of the form "a", returns absent
     */
    public Optional<QualifiedName> getPrefix()
    {
        if (parts.size() == 1) {
            return Optional.absent();
        }

        return Optional.of(QualifiedName.of(parts.subList(0, parts.size() - 1)));
    }

    public boolean hasSuffix(QualifiedName suffix)
    {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();

        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    public static Predicate<QualifiedName> hasSuffixPredicate(final QualifiedName suffix)
    {
        return new Predicate<QualifiedName>()
        {
            @Override
            public boolean apply(QualifiedName name)
            {
                return name.hasSuffix(suffix);
            }
        };
    }
}
