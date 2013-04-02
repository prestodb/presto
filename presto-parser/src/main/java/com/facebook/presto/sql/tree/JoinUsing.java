package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import java.util.List;

public class JoinUsing
        extends JoinCriteria
{
    private final List<String> columns;

    public JoinUsing(List<String> columns)
    {
        this.columns = columns;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(columns)
                .toString();
    }
}
