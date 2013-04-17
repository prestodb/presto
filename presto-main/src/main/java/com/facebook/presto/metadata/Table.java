package com.facebook.presto.metadata;

import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class Table
{
    private final long tableId;

    public Table(long tableId)
    {
        this.tableId = tableId;
    }

    public long getTableId()
    {
        return tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Table o = (Table) obj;
        return tableId == o.tableId;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tableId", tableId)
                .toString();
    }

    public static class TableMapper
            implements ResultSetMapper<Table>
    {
        public Table map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new Table(r.getLong("table_id"));
        }
    }
}
