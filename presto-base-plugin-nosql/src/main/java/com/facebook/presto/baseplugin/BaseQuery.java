package com.facebook.presto.baseplugin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 7/20/16.
 */
public class BaseQuery {
    private final BaseSplit baseSplit;
    private final ConnectorSession connectorSession;
    private final List<? extends ColumnHandle> desiredColumns;
    private final String id;

    public BaseQuery(BaseSplit baseSplit, ConnectorSession connectorSession, List<? extends ColumnHandle> desiredColumns) {
        this.baseSplit = requireNonNull(baseSplit, "baseSplit is null");
        this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
        this.desiredColumns = requireNonNull(desiredColumns, "desiredColumns is null");
        this.id = UUID.randomUUID().toString();
    }

    public BaseSplit getBaseSplit() {
        return baseSplit;
    }

    public ConnectorSession getConnectorSession() {
        return connectorSession;
    }

    public List<? extends ColumnHandle> getDesiredColumns() {
        return desiredColumns;
    }

    public String getId(){
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if(o == this){
            return true;
        }
        if(o instanceof BaseQuery){
            BaseQuery baseQuery = (BaseQuery) o;
            return new EqualsBuilder()
                    .append(baseSplit.getTableName(), baseQuery.baseSplit.getTableName())
                    .append(baseSplit.getPredicates(), baseQuery.baseSplit.getPredicates())
                    .isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(baseSplit.getTableName())
                .append(baseSplit.getPredicates())
                .toHashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("tableName", baseSplit.getTableName())
                .add("predicates", baseSplit.getPredicates())
                .toString();
    }
}
