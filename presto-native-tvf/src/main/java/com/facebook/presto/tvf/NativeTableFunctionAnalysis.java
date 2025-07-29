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
package com.facebook.presto.tvf;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class NativeTableFunctionAnalysis
{
    // a map from table argument name to list of column indexes for all columns required from the table argument
    private final Map<String, List<Integer>> requiredColumns;

    private final Optional<NativeDescriptor> returnedType;
    private final NativeTableFunctionHandle handle;

    @JsonCreator
    public NativeTableFunctionAnalysis(
            @JsonProperty("returnedType") Optional<NativeDescriptor> returnedType,
            @JsonProperty("requiredColumns") Map<String, List<Integer>> requiredColumns,
            @JsonProperty("handle") NativeTableFunctionHandle handle)
    {
        this.returnedType = requireNonNull(returnedType, "returnedType is null");
        this.requiredColumns = Collections.unmodifiableMap(
                requiredColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> Collections.unmodifiableList(entry.getValue()))));
        this.handle = requireNonNull(handle, "handle is null");
    }

    @JsonProperty
    public Optional<NativeDescriptor> getReturnedType()
    {
        return returnedType;
    }

    @JsonProperty
    public Map<String, List<Integer>> getRequiredColumns()
    {
        return requiredColumns;
    }

    @JsonProperty
    public NativeTableFunctionHandle getHandle()
    {
        return handle;
    }

    public TableFunctionAnalysis toTableFunctionAnalysis(TypeManager typeManager)
    {
        Descriptor descriptor = null;
        if (returnedType.isPresent()) {
            descriptor = new Descriptor(
                    convertToDescriptorFields(returnedType.get().getFields(), typeManager));
        }
        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .requiredColumns(requiredColumns)
                .handle(handle)
                .build();
    }

    private static List<Descriptor.Field> convertToDescriptorFields(List<NativeDescriptor.NativeField> nativeFields, TypeManager typeManager)
    {
        return nativeFields.stream()
                .map(field -> new Descriptor.Field(
                        field.getName(),
                        Optional.ofNullable(typeManager.getType(field.getTypeSignature().orElse(null)))))
                .collect(toImmutableList());
    }
}
