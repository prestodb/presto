package com.facebook.presto.hbase.metadata;

import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an HBase table in Presto.
 */
public class HbaseTable {
  private final Integer rowIdOrdinal;
  private final String schema;
  private final Optional<String> scanAuthorizations;
  private final List<ColumnMetadata> columnsMetadata;
  private final boolean indexed;
  private final List<HbaseColumnHandle> columns;
  private final String rowId;
  private final String table;
  private final SchemaTableName schemaTableName;

  @JsonCreator
  public HbaseTable(@JsonProperty("schema") String schema, @JsonProperty("table") String table,
      @JsonProperty("columns") List<HbaseColumnHandle> columns, @JsonProperty("rowId") String rowId, // rowkey字段名
      @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations) {
    this.rowId = requireNonNull(rowId, "rowId is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.table = requireNonNull(table, "table is null");
    this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns are null"));

    this.scanAuthorizations = scanAuthorizations;

    boolean indexed = false;
    Optional<Integer> rowIdOrdinal = Optional.empty();

    // Extract the ColumnMetadata from the handles for faster access
    ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
    for (HbaseColumnHandle column : this.columns) {
      columnMetadataBuilder.add(column.getColumnMetadata());
      indexed |= column.isIndexed();
      if (column.getName().equals(this.rowId)) {
        rowIdOrdinal = Optional.of(column.getOrdinal());
      }
    }
    if(rowIdOrdinal.isPresent())
      this.rowIdOrdinal = rowIdOrdinal.get();
    else
      this.rowIdOrdinal = 0;
    this.indexed = indexed;
    this.columnsMetadata = columnMetadataBuilder.build();
    this.schemaTableName = new SchemaTableName(this.schema, this.table);
  }

  @JsonProperty
  public String getRowId() {
    return rowId;
  }

  @JsonProperty
  public String getSchema() {
    return schema;
  }

  @JsonProperty
  public String getTable() {
    return table;
  }

  @JsonIgnore
  public String getFullTableName() {
    return getFullTableName(schema, table);
  }

  @JsonProperty
  public List<HbaseColumnHandle> getColumns() {
    return columns;
  }

  @JsonProperty
  public Optional<String> getScanAuthorizations() {
    return scanAuthorizations;
  }

  @JsonIgnore
  public List<ColumnMetadata> getColumnsMetadata() {
    return columnsMetadata;
  }

  @JsonIgnore
  public boolean isIndexed() {
    return indexed;
  }

  @JsonIgnore
  public int getRowIdOrdinal() {
    return this.rowIdOrdinal;
  }

  @JsonIgnore
  public static String getFullTableName(String schema, String table) {
    return schema.equals("default") ? table : schema + '.' + table;
  }

  @JsonIgnore
  public static String getFullTableName(SchemaTableName tableName) {
    return getFullTableName(tableName.getSchemaName(), tableName.getTableName());
  }

  @JsonIgnore
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("schemaName", schema).add("tableName", table)
        .add("columns", columns).add("rowIdName", rowId)
        .add("scanAuthorizations", scanAuthorizations).toString();
  }
}
