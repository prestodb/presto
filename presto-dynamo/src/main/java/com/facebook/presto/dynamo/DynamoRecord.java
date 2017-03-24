package com.facebook.presto.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.facebook.presto.baseplugin.BaseColumnHandle;
import com.facebook.presto.baseplugin.cache.BaseRecord;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Map;

/**
 * Created by amehta on 7/25/16.
 */
public class DynamoRecord implements BaseRecord {
    private final Map<String,AttributeValue> dynamoRecord;

    public DynamoRecord(Map<String, AttributeValue> dynamoRecord) {
        this.dynamoRecord = dynamoRecord;
    }

    @Override
    public boolean getBoolean(BaseColumnHandle baseColumnHandle, int field) {
        return dynamoRecord.get(baseColumnHandle.getColumnName()).getBOOL();
    }

    @Override
    public long getLong(BaseColumnHandle baseColumnHandle, int field) {
        return Long.parseLong(dynamoRecord.get(baseColumnHandle.getColumnName()).getN());
    }

    @Override
    public double getDouble(BaseColumnHandle baseColumnHandle, int field) {
        return Double.parseDouble(dynamoRecord.get(baseColumnHandle.getColumnName()).getN());
    }

    @Override
    public Slice getSlice(BaseColumnHandle baseColumnHandle, int field) {
        return Slices.utf8Slice(dynamoRecord.get(baseColumnHandle.getColumnName()).getS());
    }

    @Override
    public Object getObject(BaseColumnHandle baseColumnHandle, int field) {
        return dynamoRecord.get(baseColumnHandle.getColumnName());
    }

    @Override
    public boolean isNull(BaseColumnHandle baseColumnHandle, int field) {
        return dynamoRecord.get(baseColumnHandle.getColumnName()) == null ||
                (dynamoRecord.get(baseColumnHandle.getColumnName()).getNULL() != null && dynamoRecord.get(baseColumnHandle.getColumnName()).getNULL());
    }
}
