package com.facebook.presto.baseplugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/14/16.
 */
public class BaseConnectorInfo {
    private final String connectorId;

    @JsonCreator
    public BaseConnectorInfo(
            @JsonProperty("connectorId") String connectorId
    ){
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("connectorId", connectorId).toString();
    }
}
