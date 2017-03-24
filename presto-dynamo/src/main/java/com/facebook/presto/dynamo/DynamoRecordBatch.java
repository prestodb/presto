package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.baseplugin.cache.BaseRecord;
import com.facebook.presto.baseplugin.cache.BaseRecordBatch;

import java.util.List;
import java.util.Map;

/**
 * Created by amehta on 7/25/16.
 */
public class DynamoRecordBatch extends BaseRecordBatch {
    private final Map<String,AttributeValue> lastKeyEvaluated;

    public DynamoRecordBatch(List<BaseRecord> baseRecords, boolean isLastBatch, Map<String, AttributeValue> lastKeyEvaluated) {
        super(baseRecords, isLastBatch);
        this.lastKeyEvaluated = lastKeyEvaluated;
    }

    public Map<String, AttributeValue> getLastKeyEvaluated() {
        return lastKeyEvaluated;
    }
}
