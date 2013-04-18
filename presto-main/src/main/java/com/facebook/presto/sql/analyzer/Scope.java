package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map;

public class Scope
{
    private final Multimap<QualifiedName, TupleDescriptor> descriptors;

    public Scope(Multimap<QualifiedName, TupleDescriptor> descriptors)
    {
        this.descriptors = ImmutableMultimap.copyOf(descriptors);
    }

    public List<TupleDescriptor> getDescriptorsMatching(Optional<QualifiedName> alias)
    {
        ImmutableList.Builder<TupleDescriptor> builder = ImmutableList.builder();

        for (Map.Entry<QualifiedName, TupleDescriptor> entry : descriptors.entries()) {
            QualifiedName relationAlias = entry.getKey();
            if (!alias.isPresent() || relationAlias.hasSuffix(alias.get())) {
                builder.add(entry.getValue());
            }
        }

        return builder.build();
    }

    public List<Field> resolve(QualifiedName name)
    {
        // Namespaces can have names such as "x", "x.y" or "" if there's no name
        // Name to resolve can have names like "a", "x.a", "x.y.a"

        /*
           namespace  name     possible match
           ""         "a"           y
           "x"        "a"           y
           "x.y"      "a"           y

           ""         "x.a"         n
           "x"        "x.a"         y
           "x.y"      "x.a"         n

           ""         "x.y.a"       n
           "x"        "x.y.a"       n
           "x.y"      "x.y.a"       n

           ""         "y.a"         n
           "x"        "y.a"         n
           "x.y"      "y.a"         y
         */

        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (TupleDescriptor descriptors : getDescriptorsMatching(name.getPrefix())) {
            for (Field field : descriptors.getFields()) {
                if (field.getName().isPresent() && field.getName().get().equals(name.getSuffix())) {
                    fields.add(field);
                }
            }
        }

        return fields.build();
    }

    public boolean canResolve(QualifiedName name)
    {
        for (TupleDescriptor descriptor : getDescriptorsMatching(name.getPrefix())) {
            for (Field field : descriptor.getFields()) {
                if (field.getName().isPresent() && field.getName().get().equals(name.getSuffix())) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("descriptors", descriptors)
                .toString();
    }

    public Predicate<QualifiedName> canResolvePredicate()
    {
        return new Predicate<QualifiedName>()
        {
            @Override
            public boolean apply(QualifiedName input)
            {
                return canResolve(input);
            }
        };
    }
}
