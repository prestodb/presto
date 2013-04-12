package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class Field
{
    private final Optional<QualifiedName> prefix;
    private final Optional<String> attribute;
    private final Optional<ColumnHandle> column;
    private final Symbol symbol;
    private final Type type;

    public static final Field getField(String attribute, Symbol symbol, Type type)
    {
        return new Field(Optional.<QualifiedName>absent(), Optional.of(attribute), Optional.<ColumnHandle>absent(), symbol, type);
    }

    public Field(Optional<QualifiedName> prefix, Optional<String> attribute, Optional<ColumnHandle> column, Symbol symbol, Type type)
    {
        Preconditions.checkNotNull(prefix, "prefix is null");
        Preconditions.checkNotNull(attribute, "attribute is null");
        Preconditions.checkNotNull(column, "column is null");
        Preconditions.checkNotNull(symbol, "symbol is null");
        Preconditions.checkNotNull(type, "type is null");

        this.prefix = prefix;
        this.attribute = attribute;
        this.column = column;
        this.symbol = symbol;
        this.type = type;
    }

    public Optional<QualifiedName> getPrefix()
    {
        return prefix;
    }

    public Optional<String> getAttribute()
    {
        return attribute;
    }

    public Optional<ColumnHandle> getColumn()
    {
        return column;
    }

    public Symbol getSymbol()
    {
        return symbol;
    }

    public Type getType()
    {
        return type;
    }

    /**
     * This method can only be called if this field's prefix and attribute are present
     */
    public QualifiedName getName()
    {
        return QualifiedName.of(prefix.get(), attribute.get());
    }

    public String toString()
    {
        return String.format("%s.%s:%s:%s", prefix.or(QualifiedName.of("<anonymous>")), attribute.or("<anonymous>"), symbol, type);
    }

    public static Function<Field, QualifiedName> nameGetter()
    {
        return new Function<Field, QualifiedName>()
        {
            @Override
            public QualifiedName apply(Field input)
            {
                return input.getName();
            }
        };
    }

    public static Function<Field, Symbol> symbolGetter()
    {
        return new Function<Field, Symbol>()
        {
            @Override
            public Symbol apply(Field input)
            {
                return input.getSymbol();
            }
        };
    }

    public static Function<Field, Optional<String>> attributeGetter()
    {
        return new Function<Field, Optional<String>>()
        {
            @Override
            public Optional<String> apply(Field input)
            {
               return input.getAttribute();
            }
        };
    }
}
