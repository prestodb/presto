package com.facebook.presto.baseplugin.cache;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


/**
 * Created by amehta on 7/20/16.
 */
public class BaseRecordBatch implements Serializable{
    public static final BaseRecordBatch EMPTY_BATCH = new BaseRecordBatch(Collections.emptyList(), false);
    public static final BaseRecordBatch LAST_BATCH = new BaseRecordBatch(Collections.emptyList(), true);

    private final List<BaseRecord> baseRecords;

    private boolean isLastBatch;

    public BaseRecordBatch(List<BaseRecord> baseRecords, boolean isLastBatch) {
        this.baseRecords = baseRecords;
        this.isLastBatch = isLastBatch;
    }

    public List<BaseRecord> getBaseRecords() {
        return baseRecords;
    }

    public boolean isLastBatch() {
        return isLastBatch;
    }

    public void setLastBatch(boolean isLastBatch){
        this.isLastBatch = isLastBatch;
    }
}
