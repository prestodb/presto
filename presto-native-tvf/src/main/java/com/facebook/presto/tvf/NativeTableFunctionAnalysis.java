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

/**
 * Native implementation of table function analysis.
 */
public final class NativeTableFunctionAnalysis
{
    private final Map<String, List<Integer>> requiredColumns;
    private final Optional<NativeDescriptor> returnedType;
    private final NativeTableFunctionHandle handle;

    /**
     * Constructs a native table function analysis.
     *
     * @param returnedType the returned type descriptor
     * @param requiredColumns map from table argument name to list of
     *                        column indexes for required columns
     * @param handle the native table function handle
     */
    @JsonCreator
    public NativeTableFunctionAnalysis(
            @JsonProperty("returnedType")
            final Optional<NativeDescriptor> returnedType,
            @JsonProperty("requiredColumns")
            final Map<String, List<Integer>> requiredColumns,
            @JsonProperty("handle")
            final NativeTableFunctionHandle handle)
    {
        this.returnedType = requireNonNull(returnedType,
                "returnedType is null");
        this.requiredColumns = Collections.unmodifiableMap(
                requiredColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> Collections.unmodifiableList(
                                        entry.getValue()))));
        this.handle = requireNonNull(handle, "handle is null");
    }

    /**
     * Gets the returned type.
     *
     * @return the returned type descriptor
     */
    @JsonProperty
    public Optional<NativeDescriptor> getReturnedType()
    {
        return returnedType;
    }

    /**
     * Gets the required columns.
     *
     * @return map from table argument name to list of required column indexes
     */
    @JsonProperty
    public Map<String, List<Integer>> getRequiredColumns()
    {
        return requiredColumns;
    }

    /**
     * Gets the handle.
     *
     * @return the native table function handle
     */
    @JsonProperty
    public NativeTableFunctionHandle getHandle()
    {
        return handle;
    }

    /**
     * Converts to table function analysis.
     *
     * @param typeManager the type manager
     * @return the table function analysis
     */
    public TableFunctionAnalysis toTableFunctionAnalysis(
            final TypeManager typeManager)
    {
        Descriptor descriptor = null;
        if (returnedType.isPresent()) {
            descriptor = new Descriptor(
                    convertToDescriptorFields(
                            returnedType.get().getFields(),
                            typeManager));
        }
        TableFunctionAnalysis.Builder builder =
                TableFunctionAnalysis.builder();
        builder.returnedType(descriptor);
        for (Map.Entry<String, List<Integer>> entry
                : requiredColumns.entrySet()) {
            builder.requiredColumns(entry.getKey(), entry.getValue());
        }
        builder.handle(handle);
        return builder.build();
    }

    /**
     * Converts native fields to descriptor fields.
     *
     * @param nativeFields the native fields
     * @param typeManager the type manager
     * @return the list of descriptor fields
     */
    private static List<Descriptor.Field> convertToDescriptorFields(
            final List<NativeDescriptor.NativeField> nativeFields,
            final TypeManager typeManager)
    {
        return nativeFields.stream()
                .map(field -> new Descriptor.Field(
                        field.getName().filter(name -> !name.isEmpty()),
                        Optional.ofNullable(typeManager.getType(
                                field.getTypeSignature().orElse(null)))))
                .collect(toImmutableList());
    }
}
