package com.facebook.presto.baseplugin;

import com.facebook.presto.baseplugin.cache.BaseRecord;
import com.facebook.presto.baseplugin.cache.BaseRecordBatch;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public final class BaseRecordCursor implements RecordCursor {
    private final BaseQuery baseQuery;
    private final BaseProvider baseProvider;

    private BaseRecordBatch currentBatch;
    private BaseRecord currentRecord;
    //use index instead of iterator to support concurrency better
    private int index;

    private boolean finishedFetching;

    public BaseRecordCursor(BaseQuery baseQuery, BaseProvider baseProvider) {
        this.baseQuery = requireNonNull(baseQuery, "baseQuery is null");
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
        this.currentBatch = BaseRecordBatch.EMPTY_BATCH;
        this.index = 0;

        this.finishedFetching = currentBatch.isLastBatch();
    }

    @Override
    public long getTotalBytes() {
        return 0;
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return getColumnAtPosition(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition(){
        while(true) {
            if (index < currentBatch.getBaseRecords().size()) {
                currentRecord = currentBatch.getBaseRecords().get(index);
                index++;
                return true;
            } else {
                if (finishedFetching) {
                    return false;
                } else {//this is the fetching piece of the data
                    try {
                        currentBatch = baseProvider.getConfig().getCacheEnabled(Optional.of(baseQuery.getConnectorSession())) ? baseProvider.getRecordCache().get(baseQuery) : baseProvider.getRecordBatchForQuery(baseQuery);
                    }catch (ExecutionException e){
                        e.printStackTrace();
                        currentBatch = baseProvider.getRecordBatchForQuery(baseQuery);
                    }
                    index = 0;
                    finishedFetching = currentBatch.isLastBatch();
                }
            }
        }
    }

    @Override
    public boolean getBoolean(int field){
        return currentRecord.getBoolean(getColumnAtPosition(field), field);
    }

    @Override
    public long getLong(int field){
        return currentRecord.getLong(getColumnAtPosition(field), field);
    }

    @Override
    public double getDouble(int field){
        return currentRecord.getDouble(getColumnAtPosition(field), field);
    }

    @Override
    public Slice getSlice(int field){
        return currentRecord.getSlice(getColumnAtPosition(field), field);
    }

    @Override
    public Object getObject(int field){
        return currentRecord.getObject(getColumnAtPosition(field), field);
    }

    @Override
    public boolean isNull(int field){
        return currentRecord.isNull(getColumnAtPosition(field), field);
    }

    @Override
    public void close() {

    }

    private BaseColumnHandle getColumnAtPosition(int field){
        return ((BaseColumnHandle) baseQuery.getDesiredColumns().get(field));
    }
}
