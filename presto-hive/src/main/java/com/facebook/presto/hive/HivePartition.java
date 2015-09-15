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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.shaded.com.google.common.collect.ImmutableList;
import com.facebook.presto.hive.HivePartition.JsonMapSerializationUtil.JsonSerializableMapEntry;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveBucketing.HiveBucket;
import static java.util.Objects.requireNonNull;

public class HivePartition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final TupleDomain<HiveColumnHandle> effectivePredicate;
    private final String partitionId;
    private final Map<ColumnHandle, SerializableNativeValue> keys;
    private final Optional<HiveBucket> bucket;

    public HivePartition(SchemaTableName tableName, TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.partitionId = UNPARTITIONED_ID;
        this.keys = ImmutableMap.of();
        this.bucket = Optional.empty();
    }

    public HivePartition(SchemaTableName tableName, TupleDomain<HiveColumnHandle> effectivePredicate, Optional<HiveBucket> bucket)
    {
        this(tableName, effectivePredicate, UNPARTITIONED_ID, ImmutableMap.of(), bucket);
    }

    @JsonCreator
    public HivePartition(
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("effectivePredicate") TupleDomain<HiveColumnHandle> effectivePredicate,
            @JsonProperty("partitionId") String partitionId,
            @JsonProperty("keysEntries") List<JsonSerializableMapEntry<ColumnHandle, SerializableNativeValue>> keysEntries,
            @JsonProperty("bucket") Optional<HiveBucket> bucket)
    {
        this(tableName, effectivePredicate, partitionId, JsonMapSerializationUtil.toMap(keysEntries), bucket);
    }

    public HivePartition(
            SchemaTableName tableName,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            String partitionId,
            Map<ColumnHandle, SerializableNativeValue> keys,
            Optional<HiveBucket> bucket)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
        this.bucket = requireNonNull(bucket, "bucket number is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @JsonProperty
    public String getPartitionId()
    {
        return partitionId;
    }

    @JsonIgnore
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.withNullableFixedValues(keys);
    }

    @JsonIgnore
    public Map<ColumnHandle, SerializableNativeValue> getKeys()
    {
        return keys;
    }

    @JsonProperty
    public List<JsonSerializableMapEntry<ColumnHandle, SerializableNativeValue>> getKeysEntries()
    {
        return JsonMapSerializationUtil.toEntries(keys);
    }

    @JsonProperty
    public Optional<HiveBucket> getBucket()
    {
        return bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HivePartition other = (HivePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }

    /**
     * This helper class makes it easy to serialize/deserialize map objects in JSON.
     */
    public static final class JsonMapSerializationUtil<K, V>
    {
        private JsonMapSerializationUtil()
        {
        }

        public static <K, V> List<JsonSerializableMapEntry<K, V>> toEntries(Map<K, V> map)
        {
            ImmutableList.Builder<JsonSerializableMapEntry<K, V>> builder = ImmutableList.builder();

            for (Map.Entry<K, V> entry : map.entrySet()) {
                builder.add(new JsonSerializableMapEntry<>(entry.getKey(), entry.getValue()));
            }

            return builder.build();
        }

        public static <K, V> Map<K, V> toMap(List<JsonSerializableMapEntry<K, V>> entries)
        {
            ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
            for (JsonSerializableMapEntry<K, V> entry : entries) {
                builder.put(entry.getKey(), entry.getValue());
            }
            return builder.build();
        }

        public static class JsonSerializableMapEntry<K, V>
        {
            private final K key;
            private final V value;

            public JsonSerializableMapEntry(
                    @JsonProperty("k") K key,
                    @JsonProperty("v") V value)
            {
                this.key = key;
                this.value = value;
            }

            @JsonProperty("k")
            public K getKey()
            {
                return key;
            }

            @JsonProperty("v")
            public V getValue()
            {
                return value;
            }
        }
    }
}
