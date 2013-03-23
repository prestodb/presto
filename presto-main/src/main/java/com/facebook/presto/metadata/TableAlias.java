package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class TableAlias
{
    private final String srcCatalogName;
    private final String srcSchemaName;
    private final String srcTableName;
    private final String dstCatalogName;
    private final String dstSchemaName;
    private final String dstTableName;

    @JsonCreator
    public TableAlias(@JsonProperty("srcCatalogName") String srcCatalogName,
            @JsonProperty("srcSchemaName") String srcSchemaName,
            @JsonProperty("srcTableName") String srcTableName,
            @JsonProperty("dstCatalogName") String dstCatalogName,
            @JsonProperty("dstSchemaName") String dstSchemaName,
            @JsonProperty("dstTableName") String dstTableName)
    {
        this.srcCatalogName = srcCatalogName;
        this.srcSchemaName = srcSchemaName;
        this.srcTableName = srcTableName;

        this.dstCatalogName = dstCatalogName;
        this.dstSchemaName = dstSchemaName;
        this.dstTableName = dstTableName;
    }

    @JsonProperty
    public String getSrcCatalogName()
    {
        return srcCatalogName;
    }

    @JsonProperty
    public String getSrcSchemaName()
    {
        return srcSchemaName;
    }

    @JsonProperty
    public String getSrcTableName()
    {
        return srcTableName;
    }

    @JsonProperty
    public String getDstCatalogName()
    {
        return dstCatalogName;
    }

    @JsonProperty
    public String getDstSchemaName()
    {
        return dstSchemaName;
    }

    @JsonProperty
    public String getDstTableName()
    {
        return dstTableName;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("srcCatalogName", srcCatalogName)
                .add("srcSchemaName", srcSchemaName)
                .add("srcTableName", srcTableName)
                .add("dstCatalogName", dstCatalogName)
                .add("dstSchemaName", dstSchemaName)
                .add("dstTableName", dstTableName)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcCatalogName, srcSchemaName, srcTableName,
                dstCatalogName, dstSchemaName, dstTableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        else if ((obj == null) || (this.getClass() != obj.getClass())) {
            return false;
        }

        TableAlias other = (TableAlias) obj;
        return Objects.equal(srcCatalogName, other.srcCatalogName)
                && Objects.equal(srcSchemaName, other.srcSchemaName)
                && Objects.equal(srcTableName, other.srcTableName)
                && Objects.equal(dstCatalogName, other.dstCatalogName)
                && Objects.equal(dstSchemaName, other.dstSchemaName)
                && Objects.equal(dstTableName, other.dstTableName);
    }

    public static class TableAliasMapper implements ResultSetMapper<TableAlias>
    {
        @Override
        public TableAlias map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new TableAlias(
                r.getString("src_catalog_name"),
                r.getString("src_schema_name"),
                r.getString("src_table_name"),
                r.getString("dst_catalog_name"),
                r.getString("dst_schema_name"),
                r.getString("dst_table_name"));
        }
    }
}
