package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinUsing
        extends JoinCriteria
{
    private final List<String> columns;

    public JoinUsing(List<String> columns)
    {
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        this.columns = ImmutableList.copyOf(columns);
    }

    public List<String> getColumns()
    {
        return columns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JoinUsing o = (JoinUsing) obj;
        return Objects.equal(columns, o.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(columns);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(columns)
                .toString();
    }
}
