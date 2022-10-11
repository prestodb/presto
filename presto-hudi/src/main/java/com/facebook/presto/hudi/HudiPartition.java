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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HudiPartition
{
    private final String tableName;
    private final String partitionName;
    private final List<String> values;
    private final Map<String, String> keyValues;
    // TODO: storage and dataColumns is required from MOR record cursor, might be able to remove later
    private final Storage storage;
    private final List<HudiColumnHandle> dataColumns;
    private Map<ColumnHandle, NullableValue> keys;

    @JsonCreator
    public HudiPartition(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionName") String partitionName,
            @JsonProperty("values") List<String> values,
            @JsonProperty("keyValues") Map<String, String> keyValues,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("dataColumns") List<HudiColumnHandle> dataColumns)
    {
        this.tableName = requireNonNull(tableName, "name is null");
        this.partitionName = requireNonNull(partitionName, "name is null");
        this.values = requireNonNull(values, "values is null");
        this.keyValues = requireNonNull(keyValues, "keyValues is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.dataColumns = requireNonNull(dataColumns, "dataColumns is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getPartitionName()
    {
        return partitionName;
    }

    @JsonProperty
    public List<String> getValues()
    {
        return values;
    }

    @JsonProperty
    public Map<String, String> getKeyValues()
    {
        return keyValues;
    }

    @JsonProperty
    public Storage getStorage()
    {
        return storage;
    }

    @JsonProperty
    public List<HudiColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    public Map<ColumnHandle, NullableValue> getKeys()
    {
        return keys;
    }

    public void setKeys(Map<ColumnHandle, NullableValue> keys)
    {
        this.keys = keys;
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
        HudiPartition that = (HudiPartition) o;
        return tableName.equals(that.tableName) &&
                partitionName.equals(that.partitionName) &&
                values.equals(that.values) &&
                keyValues.equals(that.keyValues) &&
                storage.equals(that.storage) &&
                dataColumns.equals(that.dataColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, partitionName, values, keyValues, storage, dataColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("partitionName", partitionName)
                .toString();
    }
}
