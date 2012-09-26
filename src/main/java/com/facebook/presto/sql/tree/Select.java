package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class Select
{
    private final boolean distinct;
    private final boolean allColumns;
    private final List<SelectItem> selectItems;

    public Select(boolean distinct)
    {
        this.distinct = distinct;
        this.allColumns = true;
        this.selectItems = null;
    }

    public Select(boolean distinct, List<SelectItem> selectItems)
    {
        this.distinct = distinct;
        this.allColumns = false;
        this.selectItems = ImmutableList.copyOf(checkNotNull(selectItems, "selectItems"));
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public boolean isAllColumns()
    {
        return allColumns;
    }

    public List<SelectItem> getSelectItems()
    {
        checkState(!allColumns, "no select items for all columns query");
        return selectItems;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("distinct", distinct)
                .add("allColumns", allColumns)
                .add("selectItems", selectItems)
                .omitNullValues()
                .toString();
    }
}
