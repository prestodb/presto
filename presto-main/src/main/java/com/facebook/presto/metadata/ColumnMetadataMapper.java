package com.facebook.presto.metadata;

import com.facebook.presto.tuple.TupleInfo;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnMetadataMapper
        implements ResultSetMapper<ColumnMetadata>
{
    public ColumnMetadata map(int index, ResultSet r, StatementContext ctx)
            throws SQLException
    {
        TupleInfo.Type type = TupleInfo.Type.fromName(r.getString("data_type"));
        String name = r.getString("column_name");
        return new ColumnMetadata(type, name);
    }
}
