package com.facebook.presto.hbase.model;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HbaseTableHandle
    implements ConnectorInsertTableHandle, ConnectorOutputTableHandle, ConnectorTableHandle {
  private final String connectorId;
  private final String schema;
  private final String table;

  private final String rowId;
  private final Optional<String> scanAuthorizations;

  @JsonCreator
  public HbaseTableHandle(@JsonProperty("connectorId") String connectorId,
      @JsonProperty("schema") String schema, @JsonProperty("table") String table,
      @JsonProperty("rowId") String rowId,
      @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.table = requireNonNull(table, "table is null");
    this.rowId = requireNonNull(rowId, "rowId is null");

    this.scanAuthorizations = scanAuthorizations;
  }

  @JsonProperty
  public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty
  public String getRowId() {
    return rowId;
  }

  @JsonProperty
  public Optional<String> getScanAuthorizations() {
    return scanAuthorizations;
  }

  @JsonProperty
  public String getSchema() {
    return schema;
  }

  @JsonProperty
  public String getTable() {
    return table;
  }


  public SchemaTableName toSchemaTableName() {
    return new SchemaTableName(schema, table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorId, schema, table);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    HbaseTableHandle other = (HbaseTableHandle) obj;
    return Objects.equals(this.connectorId, other.connectorId)
        && Objects.equals(this.schema, other.schema) && Objects.equals(this.table, other.table)
        && Objects.equals(this.rowId, other.rowId)
        && Objects.equals(this.scanAuthorizations, other.scanAuthorizations);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("connectorId", connectorId).add("schema", schema)
        .add("table", table).add("rowId", rowId).add("scanAuthorizations", scanAuthorizations)
        .toString();
  }
}
