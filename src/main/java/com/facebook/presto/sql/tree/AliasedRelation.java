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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAliasedRelation(this, context);
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AliasedRelation that = (AliasedRelation) o;

        if (!alias.equals(that.alias)) {
            return false;
        }
        if (columnNames != null ? !columnNames.equals(that.columnNames) : that.columnNames != null) {
            return false;
        }
        if (!relation.equals(that.relation)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = relation.hashCode();
        result = 31 * result + alias.hashCode();
        result = 31 * result + (columnNames != null ? columnNames.hashCode() : 0);
        return result;
    }
}
