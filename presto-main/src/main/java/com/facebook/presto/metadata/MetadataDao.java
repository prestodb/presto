package com.facebook.presto.metadata;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface MetadataDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS tables (\n" +
            "  table_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  catalog_name VARCHAR(255) NOT NULL,\n" +
            "  schema_name VARCHAR(255) NOT NULL,\n" +
            "  table_name VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (catalog_name, schema_name, table_name)\n" +
            ")")
    void createTablesTable();

    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  column_id BIGINT PRIMARY KEY AUTO_INCREMENT,\n" +
            "  table_id BIGINT NOT NULL,\n" +
            "  column_name VARCHAR(255) NOT NULL,\n" +
            "  ordinal_position INT NOT NULL,\n" +
            "  data_type VARCHAR(255) NOT NULL,\n" +
            "  UNIQUE (table_id, column_name),\n" +
            "  UNIQUE (table_id, ordinal_position),\n" +
            "  FOREIGN KEY (table_id) REFERENCES tables (table_id)\n" +
            ")")
    void createColumnsTable();

    @SqlQuery("SELECT table_id FROM tables\n" +
            "WHERE catalog_name = :catalogName\n" +
            "  AND schema_name = :schemaName\n" +
            "  AND table_name = :tableName")
    Long getTableId(@BindBean QualifiedTableName table);

    @SqlQuery("SELECT catalog_name, schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE table_id = :tableId")
    @Mapper(QualifiedTableNameMapper.class)
    QualifiedTableName getTableName(@Bind("tableId") long tableId);

    @SqlQuery("SELECT t.catalog_name, t.schema_name, t.table_name,\n" +
            "  c.column_name, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE (c.column_id = :columnId)")
    @Mapper(TableColumnMapper.class)
    TableColumn getTableColumn(@Bind("columnId") long columnId);

    @SqlQuery("SELECT column_id, column_name, data_type\n" +
            "FROM columns\n" +
            "WHERE table_id = :tableId\n" +
            "ORDER BY ordinal_position")
    @Mapper(ColumnMetadataMapper.class)
    List<ColumnMetadata> getTableColumnMetaData(@Bind("tableId") long tableId);

    @SqlQuery("SELECT catalog_name, schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE (catalog_name = :catalogName OR :catalogName IS NULL)\n" +
            "  AND (schema_name = :schemaName OR :schemaName IS NULL)")
    @Mapper(QualifiedTableNameMapper.class)
    List<QualifiedTableName> listTables(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT DISTINCT schema_name FROM tables\n" +
            "WHERE catalog_name = :catalogName\n")
    List<String> listSchemaNames(@Bind("catalogName") String catalogName);

    @SqlQuery("SELECT t.catalog_name, t.schema_name, t.table_name,\n" +
            "  c.column_name, c.ordinal_position, c.data_type\n" +
            "FROM tables t\n" +
            "JOIN columns c ON (t.table_id = c.table_id)\n" +
            "WHERE (catalog_name = :catalogName OR :catalogName IS NULL)\n" +
            "  AND (schema_name = :schemaName OR :schemaName IS NULL)\n" +
            "  AND (table_name = :tableName OR :tableName IS NULL)\n" +
            "ORDER BY schema_name, table_name, ordinal_position")
    @Mapper(TableColumnMapper.class)
    List<TableColumn> listTableColumns(
            @Bind("catalogName") String catalogName,
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT COUNT(*) > 0 FROM tables\n" +
            "WHERE catalog_name = :catalogName\n" +
            "  AND schema_name = :schemaName\n" +
            "  AND table_name = :tableName")
    boolean tableExists(@BindBean QualifiedTableName table);

    @SqlUpdate("INSERT INTO tables (catalog_name, schema_name, table_name)\n" +
            "VALUES (:catalogName, :schemaName, :tableName)")
    @GetGeneratedKeys
    long insertTable(@BindBean QualifiedTableName table);

    @SqlUpdate("INSERT INTO columns (table_id, column_name, ordinal_position, data_type)\n" +
            "VALUES (:tableId, :columnName, :ordinalPosition, :dataType)")
    void insertColumn(
            @Bind("tableId") long tableId,
            @Bind("columnName") String columnName,
            @Bind("ordinalPosition") int ordinalPosition,
            @Bind("dataType") String dataType);
    }
