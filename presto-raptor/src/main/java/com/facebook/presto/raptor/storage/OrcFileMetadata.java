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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Long.parseLong;

public class OrcFileMetadata
{
    static final String KEY = "metadata";

    private static final JsonCodec<Map<Long, TypeSignature>> COLUMN_TYPES_CODEC =
            new JsonCodecFactory().mapJsonCodec(Long.class, TypeSignature.class);

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

    public Map<String, String> toMap()
    {
        return ImmutableMap.<String, String>builder()
                .put("columnTypes", COLUMN_TYPES_CODEC.toJson(columnTypes))
                .build();
    }

    public static OrcFileMetadata from(List<Long> columnIds, List<Type> columnTypes)
    {
        checkArgument(columnIds.size() == columnTypes.size(), "list sizes mismatch");
        ImmutableMap.Builder<Long, TypeSignature> types = ImmutableMap.builder();
        for (int i = 0; i < columnIds.size(); i++) {
            types.put(columnIds.get(i), columnTypes.get(i).getTypeSignature());
        }
        return new OrcFileMetadata(types.build());
    }

    public static OrcFileMetadata from(Map<String, Slice> metadata)
    {
        return new OrcFileMetadata(
                COLUMN_TYPES_CODEC.fromJson(metadata.get("columnTypes").toStringUtf8()));
    }

    public static OrcFileMetadata from(OrcReader reader)
    {
        return from(reader.getFooter().getUserMetadata());
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
