package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.Split;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;

public class HandleResolver
{
    private final ConcurrentMap<String, ConnectorHandleResolver> handleIdResolvers = new ConcurrentHashMap<>();

    public void addHandleResolver(String id, ConnectorHandleResolver connectorHandleResolver)
    {
        ConnectorHandleResolver existingResolver = handleIdResolvers.putIfAbsent(id, connectorHandleResolver);
        checkState(existingResolver == null, "Id %s is already assigned to resolver %s", id, existingResolver);
    }

    public String getId(TableHandle tableHandle)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(tableHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for table handle: " + tableHandle);
    }

    public String getId(ColumnHandle columnHandle)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(columnHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for column handle: " + columnHandle);
    }

    public String getId(Split split)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(split)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for split: " + split);
    }

    public Class<? extends TableHandle> getTableHandleClass(String id)
    {
        return handleIdResolvers.get(id).getTableHandleClass();
    }

    public Class<? extends ColumnHandle> getColumnHandleClass(String id)
    {
        return handleIdResolvers.get(id).getColumnHandleClass();
    }

    public Class<? extends Split> getSplitClass(String id)
    {
        return handleIdResolvers.get(id).getSplitClass();
    }
}
