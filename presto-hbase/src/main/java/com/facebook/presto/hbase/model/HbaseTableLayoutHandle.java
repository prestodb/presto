package com.facebook.presto.hbase.model;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HbaseTableLayoutHandle implements ConnectorTableLayoutHandle {
  private final HbaseTableHandle table;
  private final TupleDomain<ColumnHandle> constraint;

  @JsonCreator
  public HbaseTableLayoutHandle(@JsonProperty("table") HbaseTableHandle table,
      @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
    this.table = requireNonNull(table, "table is null");
    this.constraint = requireNonNull(constraint, "constraint is null");
  }

  @JsonProperty
  public HbaseTableHandle getTable() {
    return table;
  }

  @JsonProperty
  public TupleDomain<ColumnHandle> getConstraint() {
    return constraint;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    HbaseTableLayoutHandle other = (HbaseTableLayoutHandle) obj;
    return Objects.equals(table, other.table) && Objects.equals(constraint, other.constraint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, constraint);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("table", table).add("constraint", constraint).toString();
  }
}
