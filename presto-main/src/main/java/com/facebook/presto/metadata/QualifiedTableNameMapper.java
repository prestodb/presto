package com.facebook.presto.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class QualifiedTableNameMapper
        implements ResultSetMapper<QualifiedTableName>
{
    @Override
    public QualifiedTableName map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        return new QualifiedTableName(
                r.getString("catalog_name"),
                r.getString("schema_name"),
                r.getString("table_name"));
    }
}
