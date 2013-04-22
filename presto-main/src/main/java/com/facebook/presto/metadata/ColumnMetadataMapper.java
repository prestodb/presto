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
        String name = r.getString("column_name");
        TupleInfo.Type type = TupleInfo.Type.fromName(r.getString("data_type"));
        int ordinalPosition = r.getInt("ordinal_position");
        return new ColumnMetadata(name, type, ordinalPosition);
    }
}
