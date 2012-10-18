package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Select
    extends Node
{
    private final boolean distinct;
    private final List<Expression> selectItems;

    public Select(boolean distinct, List<Expression> selectItems)
    {
        this.distinct = distinct;
        this.selectItems = ImmutableList.copyOf(checkNotNull(selectItems, "selectItems"));
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public List<Expression> getSelectItems()
    {
        return selectItems;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSelect(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("distinct", distinct)
                .add("selectItems", selectItems)
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

        Select select = (Select) o;

        if (distinct != select.distinct) {
            return false;
        }
        if (!selectItems.equals(select.selectItems)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (distinct ? 1 : 0);
        result = 31 * result + selectItems.hashCode();
        return result;
    }
}
