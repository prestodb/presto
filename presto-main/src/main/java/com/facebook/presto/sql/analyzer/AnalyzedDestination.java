package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.QualifiedTableName;

import static com.google.common.base.Preconditions.checkNotNull;

public class AnalyzedDestination
{
    private final QualifiedTableName tableName;

    public AnalyzedDestination(QualifiedTableName tableName)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    public QualifiedTableName getTableName()
    {
        return tableName;
    }
}
