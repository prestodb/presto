package com.facebook.presto.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TableNamespaceMapper
    implements ResultSetMapper<TableNamespace>
{
    @Override
    public TableNamespace map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        return new TableNamespace(
                r.getString("catalog_name"),
                r.getString("schema_name"),
                r.getString("table_name"));
    }
}
