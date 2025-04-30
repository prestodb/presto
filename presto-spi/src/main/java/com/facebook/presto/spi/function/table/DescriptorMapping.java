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
package com.facebook.presto.spi.function.table;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.function.table.ConnectorTableFunction.checkArgument;
import static com.facebook.presto.spi.function.table.ConnectorTableFunction.checkNotNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class DescriptorMapping
{
    public static final DescriptorMapping EMPTY_MAPPING = new DescriptorMappingBuilder().build();

    private final Map<NameAndPosition, String> mappingByField;
    private final Map<String, String> mappingByDescriptor;

    private DescriptorMapping(Map<NameAndPosition, String> mappingByField, Map<String, String> mappingByDescriptor)
    {
        this.mappingByField = unmodifiableMap(new HashMap<>(requireNonNull(mappingByField, "mappingByField is null")));
        this.mappingByDescriptor = unmodifiableMap(new HashMap<>(requireNonNull(mappingByDescriptor, "mappingByDescriptor is null")));
    }

    public Map<NameAndPosition, String> getMappingByField()
    {
        return mappingByField;
    }

    public Map<String, String> getMappingByDescriptor()
    {
        return mappingByDescriptor;
    }

    public boolean isEmpty()
    {
        return mappingByField.isEmpty() && mappingByDescriptor.isEmpty();
    }

    public static class DescriptorMappingBuilder
    {
        private final Map<NameAndPosition, String> mappingByField = new HashMap<>();
        private final Map<String, String> mappingByDescriptor = new HashMap<>();
        private final Set<String> descriptorsMappedByField = new HashSet<>();

        public DescriptorMappingBuilder mapField(String descriptor, int field, String table)
        {
            checkNotNullOrEmpty(table, "table");
            checkArgument(!mappingByDescriptor.containsKey(descriptor), format("duplicate mapping for descriptor: %s, field: %s", descriptor, field));
            checkArgument(mappingByField.put(new NameAndPosition(descriptor, field), table) == null, format("duplicate mapping for descriptor: %s, field: %s", descriptor, field));
            descriptorsMappedByField.add(descriptor);
            return this;
        }

        public DescriptorMappingBuilder mapAllFields(String descriptor, String table)
        {
            checkNotNullOrEmpty(descriptor, "descriptor");
            checkNotNullOrEmpty(table, "table");
            checkArgument(!descriptorsMappedByField.contains(descriptor), "duplicate mapping for field of descriptor: " + descriptor);
            checkArgument(mappingByDescriptor.put(descriptor, table) == null, "duplicate mapping for descriptor: " + descriptor);
            return this;
        }

        public DescriptorMapping build()
        {
            return new DescriptorMapping(mappingByField, mappingByDescriptor);
        }
    }
}
