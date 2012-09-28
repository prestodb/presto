package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

public class Join
        extends Relation
{
    public Join(Type type, Relation left, Relation right, JoinCriteria criteria)
    {
        this.type = type;
        this.left = left;
        this.right = right;
        this.criteria = criteria;
    }

    public enum Type
    {
        CROSS, INNER, LEFT, RIGHT, FULL
    }

    private final Type type;
    private final Relation left;
    private final Relation right;
    private final JoinCriteria criteria;

    public Type getType()
    {
        return type;
    }

    public Relation getLeft()
    {
        return left;
    }

    public Relation getRight()
    {
        return right;
    }

    public JoinCriteria getCriteria()
    {
        return criteria;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("type", type)
                .add("left", left)
                .add("right", right)
                .add("criteria", criteria)
                .omitNullValues()
                .toString();
    }
}
