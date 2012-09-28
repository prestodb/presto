package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class AliasedRelation
        extends Relation
{
    private final Relation relation;
    private final String alias;
    private final List<String> columnNames;

    public AliasedRelation(Relation relation, String alias, List<String> columnNames)
    {
        this.relation = relation;
        this.alias = alias;
        this.columnNames = columnNames;
    }

    public Relation getRelation()
    {
        return relation;
    }

    public String getAlias()
    {
        return alias;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("relation", relation)
                .add("alias", alias)
                .add("columnNames", columnNames)
                .omitNullValues()
                .toString();
    }
}
