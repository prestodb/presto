package com.facebook.presto.hdfs;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSConnectorId {
    private final String connectorId;

    public HDFSConnectorId(String connectorId) {
        requireNonNull(connectorId, "connectorId is null");
        this.connectorId = connectorId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HDFSConnectorId other = (HDFSConnectorId) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }

    @Override
    public String toString() {
        return connectorId;
    }
}
