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
package com.facebook.presto.iceberg.partitioning;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class IcebergPartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final List<IcebergPartitionFieldHandle> partitionFieldHandles;

    @JsonCreator
    public IcebergPartitioningHandle(@JsonProperty("partitionFieldHandles") List<IcebergPartitionFieldHandle> partitionFieldHandles)
    {
        this.partitionFieldHandles = ImmutableList.copyOf(requireNonNull(partitionFieldHandles, "partitionFieldHandles is null"));
    }

    @JsonProperty
    public List<IcebergPartitionFieldHandle> getPartitionFieldHandles()
    {
        return partitionFieldHandles;
    }

    public static IcebergPartitioningHandle create(PartitionSpec spec, TypeManager typeManager)
    {
        Set<Integer> sourceColumnIdSet = spec.fields().stream().map(PartitionField::sourceId).collect(Collectors.toSet());
        Map<Integer, Integer> sourceColumnIdToIndex = new HashMap<>();
        List<Types.NestedField> fields = spec.schema().columns();
        AtomicInteger index = new AtomicInteger(0);
        for (Types.NestedField field : fields) {
            if (sourceColumnIdSet.contains(field.fieldId()) && !sourceColumnIdToIndex.containsKey(field.fieldId())) {
                sourceColumnIdToIndex.put(field.fieldId(), index.getAndIncrement());
            }
        }
        List<IcebergPartitionFieldHandle> partitionFields = spec.fields().stream()
                .map(field -> IcebergPartitionFieldHandle.create(sourceColumnIdToIndex.get(field.sourceId()),
                        field, field.transform().toString(),
                        toPrestoType(spec.schema().findType(field.sourceId()), typeManager)))
                .collect(toImmutableList());

        return new IcebergPartitioningHandle(partitionFields);
    }
}
