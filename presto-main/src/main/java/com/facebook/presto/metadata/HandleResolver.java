/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

import javax.inject.Inject;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class HandleResolver
{
    private final ConcurrentMap<String, ConnectorHandleResolver> handleIdResolvers = new ConcurrentHashMap<>();

    public HandleResolver()
    {
    }

    @Inject
    public HandleResolver(Map<String, ConnectorHandleResolver> handleIdResolvers)
    {
        this.handleIdResolvers.putAll(handleIdResolvers);
    }

    public void addHandleResolver(String id, ConnectorHandleResolver connectorHandleResolver)
    {
        ConnectorHandleResolver existingResolver = handleIdResolvers.putIfAbsent(id, connectorHandleResolver);
        checkState(existingResolver == null, "Id %s is already assigned to resolver %s", id, existingResolver);
    }

    public String getId(ConnectorTableHandle tableHandle)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(tableHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for table handle: " + tableHandle);
    }

    public String getId(ConnectorColumnHandle columnHandle)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(columnHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for column handle: " + columnHandle);
    }

    public String getId(ConnectorSplit split)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(split)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for split: " + split);
    }

    public String getId(ConnectorIndexHandle indexHandle)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleIdResolvers.entrySet()) {
            if (entry.getValue().canHandle(indexHandle)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("No connector for index handle: " + indexHandle);
    }

    public Class<? extends ConnectorTableHandle> getTableHandleClass(String id)
    {
        ConnectorHandleResolver connectorHandleResolver = handleIdResolvers.get(id);
        checkArgument(connectorHandleResolver != null, "No handle resolver for %s", id);
        return connectorHandleResolver.getTableHandleClass();
    }

    public Class<? extends ConnectorColumnHandle> getColumnHandleClass(String id)
    {
        ConnectorHandleResolver connectorHandleResolver = handleIdResolvers.get(id);
        checkArgument(connectorHandleResolver != null, "No handle resolver for %s", id);
        return connectorHandleResolver.getColumnHandleClass();
    }

    public Class<? extends ConnectorSplit> getSplitClass(String id)
    {
        ConnectorHandleResolver connectorHandleResolver = handleIdResolvers.get(id);
        checkArgument(connectorHandleResolver != null, "No handle resolver for %s", id);
        return connectorHandleResolver.getSplitClass();
    }

    public Class<? extends ConnectorIndexHandle> getIndexHandleClass(String id)
    {
        ConnectorHandleResolver connectorHandleResolver = handleIdResolvers.get(id);
        checkArgument(connectorHandleResolver != null, "No handle resolver for %s", id);
        return connectorHandleResolver.getIndexHandleClass();
    }
}
