package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseTableLayoutHandle implements ConnectorTableLayoutHandle {
    private final BaseTableHandle table;
    private final TupleDomain<ColumnHandle> summary;

    @JsonCreator
    public BaseTableLayoutHandle(
            @JsonProperty("table") BaseTableHandle table,
            @JsonProperty("summary") TupleDomain<ColumnHandle> summary
    ){
        this.table = requireNonNull(table, "table is null");
        this.summary = requireNonNull(summary, "summary is null");
    }

    @JsonProperty
    public BaseTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getSummary()
    {
        return summary;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
