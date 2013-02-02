package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

public class Symbol
{
    private final String name;

    @JsonCreator
    public Symbol(String name)
    {
        Preconditions.checkNotNull(name, "name is null");
        this.name = name;
    }

    @JsonValue
    public String getName()
    {
        return name;
    }

    public QualifiedName toQualifiedName()
    {
        return QualifiedName.of(name);
    }

    public static Symbol fromQualifiedName(QualifiedName name)
    {
        Preconditions.checkArgument(!name.getPrefix().isPresent(), "Can't create a symbol from a qualified name with prefix");
        return new Symbol(name.getSuffix());
    }

    @Override
    public String toString()
    {
        return name;
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

        Symbol symbol = (Symbol) o;

        if (!name.equals(symbol.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }
}
