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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SafeConnectorFactory;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.event.client.TypeParameterUtils.getTypeParameters;

public final class SafeTypeAdapter<
        TH extends ConnectorTableHandle,
        CH extends ConnectorColumnHandle,
        S extends ConnectorSplit,
        IH extends ConnectorIndexHandle,
        OTH extends ConnectorOutputTableHandle,
        ITH extends ConnectorInsertTableHandle,
        P extends ConnectorPartition>
        implements ConnectorHandleResolver
{
    private final Class<TH> tableHandleClass;
    private final Class<CH> columnHandleClass;
    private final Class<S> splitClass;
    private final Class<IH> indexHandleClass;
    private final Class<OTH> outputTableHandleClass;
    private final Class<ITH> insertTableHandleClass;
    private final Class<P> partitionClass;

    public SafeTypeAdapter(
            Class<TH> tableHandleClass,
            Class<CH> columnHandleClass,
            Class<S> splitClass,
            Class<IH> indexHandleClass,
            Class<OTH> outputTableHandleClass,
            Class<ITH> insertTableHandleClass,
            Class<P> partitionClass)
    {
        this.tableHandleClass = checkNotNull(tableHandleClass, "tableHandleClass is null");
        this.columnHandleClass = checkNotNull(columnHandleClass, "columnHandleClass is null");
        this.splitClass = checkNotNull(splitClass, "splitClass is null");
        this.indexHandleClass = checkNotNull(indexHandleClass, "indexHandleClass is null");
        this.outputTableHandleClass = checkNotNull(outputTableHandleClass, "outputTableHandleClass is null");
        this.insertTableHandleClass = checkNotNull(insertTableHandleClass, "insertTableHandleClass is null");
        this.partitionClass = checkNotNull(partitionClass, "partitionClass is null");
    }

    @SuppressWarnings("rawtypes")
    public SafeTypeAdapter(SafeConnectorFactory<TH, CH, S, IH, OTH, ITH, P> safeConnectorFactory)
    {
        this((Class<? extends SafeConnectorFactory<TH, CH, S, IH, OTH, ITH, P>>) checkNotNull(safeConnectorFactory, "safeConnectorFactory is null").getClass(),
                getTypeParameters(safeConnectorFactory.getClass(), SafeConnectorFactory.class));
    }

    private SafeTypeAdapter(Class<? extends SafeConnectorFactory<TH, CH, S, IH, OTH, ITH, P>> safeConnectorFactoryClass, Type[] typeParameters)
    {
        this.tableHandleClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[0], ConnectorTableHandle.class);
        this.columnHandleClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[1], ConnectorColumnHandle.class);
        this.splitClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[2], ConnectorSplit.class);
        this.indexHandleClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[3], ConnectorIndexHandle.class);
        this.outputTableHandleClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[4], ConnectorOutputTableHandle.class);
        this.insertTableHandleClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[5], ConnectorInsertTableHandle.class);
        this.partitionClass = checkTypeParameter(safeConnectorFactoryClass, typeParameters[6], ConnectorPartition.class);
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandleClass.isInstance(tableHandle);
    }

    @Override
    public boolean canHandle(ConnectorColumnHandle columnHandle)
    {
        return columnHandleClass.isInstance(columnHandle);
    }

    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return splitClass.isInstance(split);
    }

    @Override
    public boolean canHandle(ConnectorIndexHandle indexHandle)
    {
        return indexHandleClass.isInstance(indexHandle);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle outputTableHandle)
    {
        return outputTableHandleClass.isInstance(outputTableHandle);
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle insertTableHandle)
    {
        return insertTableHandleClass.isInstance(insertTableHandle);
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return tableHandleClass;
    }

    @Override
    public Class<? extends ConnectorColumnHandle> getColumnHandleClass()
    {
        return columnHandleClass;
    }

    @Override
    public Class<? extends ConnectorIndexHandle> getIndexHandleClass()
    {
        return indexHandleClass;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return splitClass;
    }

    public IH castIndexHandle(ConnectorIndexHandle indexHandle)
    {
        return indexHandleClass.cast(indexHandle);
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return outputTableHandleClass;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return insertTableHandleClass;
    }

    public TH castTableHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandleClass.cast(tableHandle);
    }

    public CH castColumnHandle(ConnectorColumnHandle columnHandle)
    {
        return columnHandleClass.cast(columnHandle);
    }

    public List<CH> castColumnHandles(List<? extends ConnectorColumnHandle> columns)
    {
        // todo is there a safe way to do this?
        return (List<CH>) columns;
    }

    public Set<CH> castColumnHandlesSet(Set<? extends ConnectorColumnHandle> columns)
    {
        // todo is there a safe way to do this?
        return (Set<CH>) columns;
    }

    public <K> Map<K, ConnectorColumnHandle> castColumnHandleMap(Map<K, CH> columnHandleMap)
    {
        // cast is safe because ImmutableMap is read only
        return (Map<K, ConnectorColumnHandle>) ImmutableMap.copyOf(columnHandleMap);
    }

    public S castSplit(ConnectorSplit split)
    {
        return splitClass.cast(split);
    }

    public OTH castOutputTableHandle(ConnectorOutputTableHandle outputTableHandle)
    {
        return outputTableHandleClass.cast(outputTableHandle);
    }

    public ITH castInsertTableHandle(ConnectorInsertTableHandle insertTableHandle)
    {
        return insertTableHandleClass.cast(insertTableHandle);
    }

    public List<P> castPartitions(List<ConnectorPartition> partitions)
    {
        // todo is there a safe way to do this?
        return (List<P>) partitions;
    }

    public TupleDomain<CH> castTupleDomain(TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        // todo is there a safe way to do this?
        return (TupleDomain<CH>) tupleDomain;
    }

    public TupleDomain<ConnectorColumnHandle> castToGenericTupleDomain(TupleDomain<CH> tupleDomain)
    {
        // todo is there a safe way to do this?
        return (TupleDomain<ConnectorColumnHandle>) tupleDomain;
    }

    @SuppressWarnings("ConstantConditions")
    public <T> Class<T> checkTypeParameter(
            Class<? extends SafeConnectorFactory<TH, CH, S, IH, OTH, ITH, P>> safeConnectorFactoryClass,
            Type typeParameter,
            Class<?> requiredSuperClass)
    {
        if (typeParameter instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) typeParameter;
            return checkTypeParameter(safeConnectorFactoryClass, parameterizedType.getRawType(), requiredSuperClass);
        }

        checkArgument(typeParameter instanceof Class,
                "%s type parameter of %s is not a concrete class, but is %s",
                requiredSuperClass.getSimpleName(),
                safeConnectorFactoryClass.getSimpleName(),
                typeParameter);

        Class<?> baseClass = (Class<?>) typeParameter;
        checkArgument(requiredSuperClass.isAssignableFrom(baseClass), "Type is not a concrete class");
        return (Class<T>) baseClass;
    }
}
