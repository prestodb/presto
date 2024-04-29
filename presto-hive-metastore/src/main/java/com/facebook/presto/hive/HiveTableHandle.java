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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveTableHandle
        extends BaseHiveTableHandle
{
    private final Optional<List<List<String>>> analyzePartitionValues;

    @JsonCreator
    public HiveTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("analyzePartitionValues") Optional<List<List<String>>> analyzePartitionValues)
    {
        super(schemaName, tableName);

        this.analyzePartitionValues = requireNonNull(analyzePartitionValues, "analyzePartitionValues is null");
    }

    public HiveTableHandle(String schemaName, String tableName)
    {
        this(schemaName, tableName, Optional.empty());
    }

    public HiveTableHandle withAnalyzePartitionValues(Optional<List<List<String>>> analyzePartitionValues)
    {
        return new HiveTableHandle(getSchemaName(), getTableName(), analyzePartitionValues);
    }

    @JsonProperty
    public Optional<List<List<String>>> getAnalyzePartitionValues()
    {
        return analyzePartitionValues;
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
        HiveTableHandle that = (HiveTableHandle) o;
        // Do not include analyzePartitionValues in hashCode and equals comparison
        return Objects.equals(getSchemaName(), that.getSchemaName()) &&
                Objects.equals(getTableName(), that.getTableName());
    }

    @Override
    public int hashCode()
    {
        // Do not include analyzePartitionValues in hashCode and equals comparison
        return Objects.hash(getSchemaName(), getTableName());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", getSchemaName())
                .add("tableName", getTableName())
                .add("analyzePartitionValues", analyzePartitionValues)
                .toString();
    }
}
