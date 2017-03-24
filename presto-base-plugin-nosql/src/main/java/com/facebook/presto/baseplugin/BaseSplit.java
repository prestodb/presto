package com.facebook.presto.baseplugin;

import com.facebook.presto.baseplugin.predicate.BasePredicate;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseSplit implements ConnectorSplit {
    private final List<HostAddress> addresses;
    private final SchemaTableName tableName;
    private List<BasePredicate> predicates;

    @JsonCreator
    public BaseSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("predicates") List<BasePredicate> predicates)
    {
        this.addresses = requireNonNull(addresses, "address is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.predicates = requireNonNull(predicates, "predicates is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<BasePredicate> getPredicates()
    {
        return predicates;
    }

    public BaseSplit setPredicates(List<BasePredicate> predicates) {
        this.predicates = predicates;
        return this;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .add("tableName", tableName)
                .add("predicates", predicates)
                .toString();
    }
}
