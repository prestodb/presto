package com.facebook.presto.hbase;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author spancer.ray
 *
 */
public final class HbaseConnectorId {
  private final String id;

  public HbaseConnectorId(String id) {
    this.id = requireNonNull(id, "id is null");
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    return Objects.equals(this.id, ((HbaseConnectorId) obj).id);
  }
}
