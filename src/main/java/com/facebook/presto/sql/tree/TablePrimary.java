package com.facebook.presto.sql.tree;

public abstract class TablePrimary
    extends TableReference
{
    private final TableCorrelation correlation;

    protected TablePrimary(TableCorrelation correlation)
    {
        this.correlation = correlation;
    }

    public TableCorrelation getCorrelation()
    {
        return correlation;
    }
}
