package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;

public final class TableAlias
{
    private final QualifiedTableName srcTable;
    private final QualifiedTableName dstTable;

    public static TableAlias createTableAlias(QualifiedTableName srcTable, QualifiedTableName dstTable)
    {
        return new TableAlias(srcTable, dstTable);
    }

    @JsonCreator
    public TableAlias(@JsonProperty("srcCatalogName") String srcCatalogName,
            @JsonProperty("srcSchemaName") String srcSchemaName,
            @JsonProperty("srcTableName") String srcTableName,
            @JsonProperty("dstCatalogName") String dstCatalogName,
            @JsonProperty("dstSchemaName") String dstSchemaName,
            @JsonProperty("dstTableName") String dstTableName)
    {
        this(new QualifiedTableName(srcCatalogName, srcSchemaName, srcTableName),
                new QualifiedTableName(dstCatalogName, dstSchemaName, dstTableName));
    }

    private TableAlias(QualifiedTableName srcTable, QualifiedTableName dstTable)
    {
        this.srcTable = checkTable(srcTable);
        this.dstTable = checkTable(dstTable);
    }

    @JsonProperty
    public String getSrcCatalogName()
    {
        return srcTable.getCatalogName();
    }

    @JsonProperty
    public String getSrcSchemaName()
    {
        return srcTable.getSchemaName();
    }

    @JsonProperty
    public String getSrcTableName()
    {
        return srcTable.getTableName();
    }

    @JsonProperty
    public String getDstCatalogName()
    {
        return dstTable.getCatalogName();
    }

    @JsonProperty
    public String getDstSchemaName()
    {
        return dstTable.getSchemaName();
    }

    @JsonProperty
    public String getDstTableName()
    {
        return dstTable.getTableName();
    }

    @JsonIgnore
    public QualifiedTableName getSrcTable()
    {
        return srcTable;
    }

    @JsonIgnore
    public QualifiedTableName getDstTable()
    {
        return dstTable;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("srcTable", srcTable)
                .add("dstTable", dstTable)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(srcTable, dstTable);
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
        return Objects.equal(srcTable, other.srcTable)
                && Objects.equal(dstTable, other.dstTable);
    }

    public static class TableAliasMapper implements ResultSetMapper<TableAlias>
    {
        @Override
        public TableAlias map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            QualifiedTableName srcTable = new QualifiedTableName(r.getString("src_catalog_name"),
                    r.getString("src_schema_name"),
                    r.getString("src_table_name"));

            QualifiedTableName dstTable = new QualifiedTableName(r.getString("dst_catalog_name"),
                    r.getString("dst_schema_name"),
                    r.getString("dst_table_name"));


            return new TableAlias(srcTable, dstTable);
        }
    }
}
