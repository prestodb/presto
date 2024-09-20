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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PrestoIcebergPartitionSpec
{
    private final int specId;
    private final PrestoIcebergSchema schema;
    private final List<String> fields;

    @JsonCreator
    public PrestoIcebergPartitionSpec(
            @JsonProperty("specId") int specId,
            @JsonProperty("schema") PrestoIcebergSchema schema,
            @JsonProperty("fields") List<String> fields)
    {
        this.specId = specId;
        this.schema = requireNonNull(schema, "schema is null");
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @JsonProperty
    public int getSpecId()
    {
        return specId;
    }

    @JsonProperty
    public PrestoIcebergSchema getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<String> getFields()
    {
        return fields;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specId, schema, fields);
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

        PrestoIcebergPartitionSpec other = (PrestoIcebergPartitionSpec) obj;
        return Objects.equals(this.specId, other.specId) &&
                Objects.equals(this.schema, other.schema) &&
                Objects.equals(this.fields, other.fields);
    }
}
