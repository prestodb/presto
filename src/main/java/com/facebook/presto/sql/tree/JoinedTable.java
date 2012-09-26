package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class JoinedTable
        extends TablePrimary
{
    private final TableReference tableReference;

    public JoinedTable(TableReference tableReference, TableCorrelation correlation)
    {
        super(correlation);
        this.tableReference = checkNotNull(tableReference, "tableReference");
    }

    public TableReference getTableReference()
    {
        return tableReference;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableReference", tableReference)
                .add("correlation", getCorrelation())
                .omitNullValues()
                .toString();
    }
}
