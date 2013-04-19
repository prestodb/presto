package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class Field
{
    private final Optional<QualifiedName> relationAlias;
    private final Optional<String> name;
    private final Type type;
    private final int index;

    private final int relationId; // used for equality checks when relationAlias is missing


    public Field(QualifiedName relationAlias, Optional<String> name, Type type, int index)
    {
        checkNotNull(relationAlias, "relationAlias is null");
        checkNotNull(name, "name is null");
        checkNotNull(type, "type is null");

        this.index = index;
        this.relationAlias = Optional.of(relationAlias);
        this.name = name;
        this.type = type;
        this.relationId = 0;
    }

    public Field(int relationId, Optional<String> name, Type type, int index)
    {
        this.index = index;
        this.relationAlias = Optional.absent();
        this.name = name;
        this.type = type;
        this.relationId = relationId;
    }

    public Optional<QualifiedName> getRelationAlias()
    {
        return relationAlias;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public int getIndex()
    {
        return index;
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

        Field field = (Field) o;

        if (index != field.index) {
            return false;
        }
        if (relationId != field.relationId) {
            return false;
        }
        if (!name.equals(field.name)) {
            return false;
        }
        if (!relationAlias.equals(field.relationAlias)) {
            return false;
        }
        if (type != field.type) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = relationAlias.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + index;
        result = 31 * result + relationId;
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        if (relationAlias.isPresent()) {
            result.append(relationAlias.get())
                    .append(".");
        }

        result.append(name.or("@" + index))
                .append(":")
                .append(type);

        return result.toString();
    }

}
