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
package io.prestosql.plugin.raptor.legacy.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.TypeSignature;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class OrcFileMetadata
{
    static final String KEY = "metadata";

    private final Map<Long, TypeSignature> columnTypes;

    @JsonCreator
    public OrcFileMetadata(@JsonProperty("columnTypes") Map<Long, TypeSignature> columnTypes)
    {
        this.columnTypes = ImmutableMap.copyOf(columnTypes);
    }

    @JsonProperty
    public Map<Long, TypeSignature> getColumnTypes()
    {
        return columnTypes;
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
        OrcFileMetadata that = (OrcFileMetadata) o;
        return Objects.equals(columnTypes, that.columnTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnTypes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnTypes", columnTypes)
                .toString();
    }
}
