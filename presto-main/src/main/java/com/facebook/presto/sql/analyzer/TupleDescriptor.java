package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

public class TupleDescriptor
{
    private final List<Field> fields;

    public TupleDescriptor(List<Field> fields)
    {
        Preconditions.checkNotNull(fields, "fields is null");
        this.fields = ImmutableList.copyOf(fields);
    }

    public TupleDescriptor(List<Optional<String>> attributes, List<Symbol> symbols, List<Type> types)
    {
        Preconditions.checkNotNull(attributes, "attributes is null");
        Preconditions.checkNotNull(symbols, "symbols is null");
        Preconditions.checkNotNull(types, "types is null");
        Preconditions.checkArgument(attributes.size() == symbols.size(), "attributes and symbols sizes do not match");
        Preconditions.checkArgument(attributes.size() == types.size(), "attributes and types sizes do not match");

        ImmutableList.Builder<Field> builder = ImmutableList.builder();
        for (int i = 0; i < attributes.size(); i++) {
            builder.add(new Field(Optional.<QualifiedName>absent(), attributes.get(i), Optional.<ColumnHandle>absent(), symbols.get(i), types.get(i)));
        }

        this.fields = builder.build();
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public Map<Symbol, Type> getSymbols()
    {
        ImmutableMap.Builder<Symbol, Type> builder = ImmutableMap.builder();
        for (Field field : fields) {
            builder.put(field.getSymbol(), field.getType());
        }
        return builder.build();
    }

    @Override
    public String toString()
    {
        return fields.toString();
    }

    public List<Field> resolve(final QualifiedName name)
    {
        return ImmutableList.copyOf(Iterables.filter(fields, new Predicate<Field>()
        {
            @Override
            public boolean apply(Field input)
            {
                if (!input.getPrefix().isPresent() || !input.getAttribute().isPresent()) {
                    return false;
                }

                return QualifiedName.of(input.getPrefix().get(), input.getAttribute().get()).hasSuffix(name);
            }
        }));
    }
}
