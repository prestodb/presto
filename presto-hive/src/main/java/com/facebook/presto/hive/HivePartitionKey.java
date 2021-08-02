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
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class HivePartitionKey
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HivePartitionKey.class).instanceSize() +
            ClassLayout.parseClass(String.class).instanceSize() * 2;

    private final String name;
    @Nullable
    private final String value;

    @JsonCreator
    public HivePartitionKey(
            @JsonProperty("name") String name,
            @JsonProperty("value") Optional<String> value)
    {
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null").orElse(null);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getValue()
    {
        return Optional.ofNullable(value);
    }

    public int getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE + name.length() * Character.BYTES + (value == null ? 0 : value.length() * Character.BYTES);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
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
                Objects.equals(this.value, other.value);
    }
}
