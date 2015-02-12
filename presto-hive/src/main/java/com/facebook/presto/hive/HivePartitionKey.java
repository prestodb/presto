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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public final class HivePartitionKey
{
    public static final String HIVE_DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    private final String name;
    private final HiveType hiveType;
    private final String value;

    @JsonCreator
    public HivePartitionKey(
            @JsonProperty("name") String name,
            @JsonProperty("hiveType") HiveType hiveType,
            @JsonProperty("value") String value)
    {
        checkNotNull(name, "name is null");
        checkNotNull(hiveType, "hiveType is null");
        checkNotNull(value, "value is null");

        this.name = name;
        this.hiveType = hiveType;
        this.value = value.equals(HIVE_DEFAULT_DYNAMIC_PARTITION) ? "\\N" : value;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public HiveType getHiveType()
    {
        return hiveType;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("hiveType", hiveType)
                .add("value", value)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, hiveType, value);
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
        HivePartitionKey other = (HivePartitionKey) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.hiveType, other.hiveType) &&
                Objects.equals(this.value, other.value);
    }
}
