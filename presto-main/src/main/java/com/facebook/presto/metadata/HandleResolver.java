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

import com.facebook.presto.connector.informationSchema.InformationSchemaHandleResolver;
import com.facebook.presto.connector.system.SystemHandleResolver;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HandleResolver
{
    private final ConcurrentMap<String, ConnectorHandleResolver> handleResolvers = new ConcurrentHashMap<>();

    @Inject
    public HandleResolver()
    {
        handleResolvers.put(REMOTE_CONNECTOR_ID.toString(), new RemoteHandleResolver());
        handleResolvers.put("$system", new SystemHandleResolver());
        handleResolvers.put("$info_schema", new InformationSchemaHandleResolver());
    }

    public void addConnectorName(String name, ConnectorHandleResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        ConnectorHandleResolver existingResolver = handleResolvers.putIfAbsent(name, resolver);
        checkState(existingResolver == null || existingResolver.equals(resolver),
                "Connector '%s' is already assigned to resolver: %s", name, existingResolver);
    }

    public String getId(ConnectorTableHandle tableHandle)
    {
        return getId(tableHandle, ConnectorHandleResolver::getTableHandleClass);
    }

    public String getId(ConnectorTableLayoutHandle handle)
    {
        return getId(handle, ConnectorHandleResolver::getTableLayoutHandleClass);
    }

    public String getId(ColumnHandle columnHandle)
    {
        return getId(columnHandle, ConnectorHandleResolver::getColumnHandleClass);
    }

    public String getId(ConnectorSplit split)
    {
        return getId(split, ConnectorHandleResolver::getSplitClass);
    }

    public String getId(ConnectorIndexHandle indexHandle)
    {
        return getId(indexHandle, ConnectorHandleResolver::getIndexHandleClass);
    }

    public String getId(ConnectorOutputTableHandle outputHandle)
    {
        return getId(outputHandle, ConnectorHandleResolver::getOutputTableHandleClass);
    }

    public String getId(ConnectorInsertTableHandle insertHandle)
    {
        return getId(insertHandle, ConnectorHandleResolver::getInsertTableHandleClass);
    }

    public String getId(ConnectorPartitioningHandle partitioningHandle)
    {
        return getId(partitioningHandle, ConnectorHandleResolver::getPartitioningHandleClass);
    }

    public String getId(ConnectorTransactionHandle transactionHandle)
    {
        return getId(transactionHandle, ConnectorHandleResolver::getTransactionHandleClass);
    }

    public Class<? extends ConnectorTableHandle> getTableHandleClass(String id)
    {
        return resolverFor(id).getTableHandleClass();
    }

    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass(String id)
    {
        return resolverFor(id).getTableLayoutHandleClass();
    }

    public Class<? extends ColumnHandle> getColumnHandleClass(String id)
    {
        return resolverFor(id).getColumnHandleClass();
    }

    public Class<? extends ConnectorSplit> getSplitClass(String id)
    {
        return resolverFor(id).getSplitClass();
    }

    public Class<? extends ConnectorIndexHandle> getIndexHandleClass(String id)
    {
        return resolverFor(id).getIndexHandleClass();
    }

    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass(String id)
    {
        return resolverFor(id).getOutputTableHandleClass();
    }

    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass(String id)
    {
        return resolverFor(id).getInsertTableHandleClass();
    }

    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass(String id)
    {
        return resolverFor(id).getPartitioningHandleClass();
    }

    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass(String id)
    {
        return resolverFor(id).getTransactionHandleClass();
    }

    public ConnectorHandleResolver resolverFor(String id)
    {
        ConnectorHandleResolver resolver = handleResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for connector: %s", id);
        return resolver;
    }

    private <T> String getId(T handle, Function<ConnectorHandleResolver, Class<? extends T>> getter)
    {
        for (Entry<String, ConnectorHandleResolver> entry : handleResolvers.entrySet()) {
            try {
                if (getter.apply(entry.getValue()).isInstance(handle)) {
                    return entry.getKey();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No connector for handle: " + handle);
    }
}
