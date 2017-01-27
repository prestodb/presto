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

import com.facebook.presto.connector.EmptySplitHandleResolver;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HandleResolver
{
    private final ConcurrentMap<String, MaterializedHandleResolver> handleResolvers = new ConcurrentHashMap<>();

    @Inject
    public HandleResolver()
    {
        handleResolvers.put(REMOTE_CONNECTOR_ID.toString(), new MaterializedHandleResolver(new RemoteHandleResolver()));
        handleResolvers.put("$system", new MaterializedHandleResolver(new SystemHandleResolver()));
        handleResolvers.put("$info_schema", new MaterializedHandleResolver(new InformationSchemaHandleResolver()));
        handleResolvers.put("$empty", new MaterializedHandleResolver(new EmptySplitHandleResolver()));
    }

    public void addConnectorName(String name, ConnectorHandleResolver resolver)
    {
        requireNonNull(name, "name is null");
        requireNonNull(resolver, "resolver is null");
        MaterializedHandleResolver existingResolver = handleResolvers.putIfAbsent(name, new MaterializedHandleResolver(resolver));
        checkState(existingResolver == null || existingResolver.equals(resolver),
                "Connector '%s' is already assigned to resolver: %s", name, existingResolver);
    }

    public String getId(ConnectorTableHandle tableHandle)
    {
        return getId(tableHandle, MaterializedHandleResolver::getTableHandleClass);
    }

    public String getId(ConnectorTableLayoutHandle handle)
    {
        return getId(handle, MaterializedHandleResolver::getTableLayoutHandleClass);
    }

    public String getId(ColumnHandle columnHandle)
    {
        return getId(columnHandle, MaterializedHandleResolver::getColumnHandleClass);
    }

    public String getId(ConnectorSplit split)
    {
        return getId(split, MaterializedHandleResolver::getSplitClass);
    }

    public String getId(ConnectorIndexHandle indexHandle)
    {
        return getId(indexHandle, MaterializedHandleResolver::getIndexHandleClass);
    }

    public String getId(ConnectorOutputTableHandle outputHandle)
    {
        return getId(outputHandle, MaterializedHandleResolver::getOutputTableHandleClass);
    }

    public String getId(ConnectorInsertTableHandle insertHandle)
    {
        return getId(insertHandle, MaterializedHandleResolver::getInsertTableHandleClass);
    }

    public String getId(ConnectorPartitioningHandle partitioningHandle)
    {
        return getId(partitioningHandle, MaterializedHandleResolver::getPartitioningHandleClass);
    }

    public String getId(ConnectorTransactionHandle transactionHandle)
    {
        return getId(transactionHandle, MaterializedHandleResolver::getTransactionHandleClass);
    }

    public Class<? extends ConnectorTableHandle> getTableHandleClass(String id)
    {
        return resolverFor(id).getTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass(String id)
    {
        return resolverFor(id).getTableLayoutHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ColumnHandle> getColumnHandleClass(String id)
    {
        return resolverFor(id).getColumnHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorSplit> getSplitClass(String id)
    {
        return resolverFor(id).getSplitClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorIndexHandle> getIndexHandleClass(String id)
    {
        return resolverFor(id).getIndexHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass(String id)
    {
        return resolverFor(id).getOutputTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass(String id)
    {
        return resolverFor(id).getInsertTableHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass(String id)
    {
        return resolverFor(id).getPartitioningHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass(String id)
    {
        return resolverFor(id).getTransactionHandleClass().orElseThrow(() -> new IllegalArgumentException("No resolver for " + id));
    }

    private MaterializedHandleResolver resolverFor(String id)
    {
        MaterializedHandleResolver resolver = handleResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for connector: %s", id);
        return resolver;
    }

    private <T> String getId(T handle, Function<MaterializedHandleResolver, Optional<Class<? extends T>>> getter)
    {
        for (Entry<String, MaterializedHandleResolver> entry : handleResolvers.entrySet()) {
            try {
                if (getter.apply(entry.getValue()).map(clazz -> clazz.isInstance(handle)).orElse(false)) {
                    return entry.getKey();
                }
            }
            catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No connector for handle: " + handle);
    }

    private static class MaterializedHandleResolver
    {
        private final Optional<Class<? extends ConnectorTableHandle>> tableHandle;
        private final Optional<Class<? extends ConnectorTableLayoutHandle>> layoutHandle;
        private final Optional<Class<? extends ColumnHandle>> columnHandle;
        private final Optional<Class<? extends ConnectorSplit>> split;
        private final Optional<Class<? extends ConnectorIndexHandle>> indexHandle;
        private final Optional<Class<? extends ConnectorOutputTableHandle>> outputTableHandle;
        private final Optional<Class<? extends ConnectorInsertTableHandle>> insertTableHandle;
        private final Optional<Class<? extends ConnectorPartitioningHandle>> partitioningHandle;
        private final Optional<Class<? extends ConnectorTransactionHandle>> transactionHandle;

        public MaterializedHandleResolver(ConnectorHandleResolver resolver)
        {
            tableHandle = getHandleClass(resolver::getTableHandleClass);
            layoutHandle = getHandleClass(resolver::getTableLayoutHandleClass);
            columnHandle = getHandleClass(resolver::getColumnHandleClass);
            split = getHandleClass(resolver::getSplitClass);
            indexHandle = getHandleClass(resolver::getIndexHandleClass);
            outputTableHandle = getHandleClass(resolver::getOutputTableHandleClass);
            insertTableHandle = getHandleClass(resolver::getInsertTableHandleClass);
            partitioningHandle = getHandleClass(resolver::getPartitioningHandleClass);
            transactionHandle = getHandleClass(resolver::getTransactionHandleClass);
        }

        private static <T> Optional<Class<? extends T>> getHandleClass(Supplier<Class<? extends T>> callable)
        {
            try {
                return Optional.of(callable.get());
            }
            catch (UnsupportedOperationException e) {
                return Optional.empty();
            }
        }

        public Optional<Class<? extends ConnectorTableHandle>> getTableHandleClass()
        {
            return tableHandle;
        }

        public Optional<Class<? extends ConnectorTableLayoutHandle>> getTableLayoutHandleClass()
        {
            return layoutHandle;
        }

        public Optional<Class<? extends ColumnHandle>> getColumnHandleClass()
        {
            return columnHandle;
        }

        public Optional<Class<? extends ConnectorSplit>> getSplitClass()
        {
            return split;
        }

        public Optional<Class<? extends ConnectorIndexHandle>> getIndexHandleClass()
        {
            return indexHandle;
        }

        public Optional<Class<? extends ConnectorOutputTableHandle>> getOutputTableHandleClass()
        {
            return outputTableHandle;
        }

        public Optional<Class<? extends ConnectorInsertTableHandle>> getInsertTableHandleClass()
        {
            return insertTableHandle;
        }

        public Optional<Class<? extends ConnectorPartitioningHandle>> getPartitioningHandleClass()
        {
            return partitioningHandle;
        }

        public Optional<Class<? extends ConnectorTransactionHandle>> getTransactionHandleClass()
        {
            return transactionHandle;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedHandleResolver that = (MaterializedHandleResolver) o;
            return Objects.equals(tableHandle, that.tableHandle) &&
                    Objects.equals(layoutHandle, that.layoutHandle) &&
                    Objects.equals(columnHandle, that.columnHandle) &&
                    Objects.equals(split, that.split) &&
                    Objects.equals(indexHandle, that.indexHandle) &&
                    Objects.equals(outputTableHandle, that.outputTableHandle) &&
                    Objects.equals(insertTableHandle, that.insertTableHandle) &&
                    Objects.equals(partitioningHandle, that.partitioningHandle) &&
                    Objects.equals(transactionHandle, that.transactionHandle);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle, layoutHandle, columnHandle, split, indexHandle, outputTableHandle, insertTableHandle, partitioningHandle, transactionHandle);
        }
    }
}
