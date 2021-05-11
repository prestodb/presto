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
package com.facebook.presto.kafka.schema;

import com.facebook.presto.kafka.KafkaTopicDescription;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class MapBasedTableDescriptionSupplier
        implements TableDescriptionSupplier
{
    private final Map<SchemaTableName, KafkaTopicDescription> map;

    public MapBasedTableDescriptionSupplier(Map<SchemaTableName, KafkaTopicDescription> map)
    {
        this.map = ImmutableMap.copyOf(requireNonNull(map, "map is null"));
    }

    @Override
    public Set<SchemaTableName> listTables()
    {
        return map.keySet();
    }

    @Override
    public Optional<KafkaTopicDescription> getTopicDescription(SchemaTableName schemaTableName)
    {
        return Optional.ofNullable(map.get(schemaTableName));
    }
}
