package com.facebook.presto.baseplugin.cache;

import com.facebook.presto.baseplugin.BaseColumnHandle;
import io.airlift.slice.Slice;

import java.io.Serializable;

/**
 * Created by amehta on 7/19/16.
 */
public interface BaseRecord extends Serializable{
    boolean getBoolean(BaseColumnHandle baseColumnHandle, int field);

    long getLong(BaseColumnHandle baseColumnHandle, int field);

    double getDouble(BaseColumnHandle baseColumnHandle, int field);

    Slice getSlice(BaseColumnHandle baseColumnHandle, int field);

    Object getObject(BaseColumnHandle baseColumnHandle, int field);

    boolean isNull(BaseColumnHandle baseColumnHandle, int field);
}
