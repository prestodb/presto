package com.facebook.presto.metadata;

import java.util.List;

public interface InternalSchemaMetadata
{
    TableMetadata getTable(QualifiedTableName table);

    List<QualifiedTableName> listTables(String catalogName);
}
