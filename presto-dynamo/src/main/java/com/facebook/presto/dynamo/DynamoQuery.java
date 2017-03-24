package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.baseplugin.BaseQuery;
import com.facebook.presto.baseplugin.BaseSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by amehta on 7/25/16.
 */
public class DynamoQuery extends BaseQuery {
    private Optional<Integer> limit;
    private Optional<Map<String,AttributeValue>> offset;

    public DynamoQuery(BaseSplit baseSplit, ConnectorSession connectorSession, List<? extends ColumnHandle> desiredColumns) {
        super(baseSplit, connectorSession, desiredColumns);
        this.offset = Optional.empty();
        //set this empty until limit pushdown enabled
        this.limit = Optional.empty();
    }

    public Optional<Integer> getLimit() {
        return limit;
    }

    public DynamoQuery setLimit(Optional<Integer> limit) {
        this.limit = limit;
        return this;
    }

    public Optional<Map<String, AttributeValue>> getOffset() {
        return offset;
    }

    public DynamoQuery setOffset(Optional<Map<String, AttributeValue>> offset) {
        this.offset = offset;
        return this;
    }
}
