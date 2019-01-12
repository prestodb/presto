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
package com.facebook.presto.redis;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a single field from a Redis key/value row. See {@link RedisTableDescription} for more details.
 */
public final class RedisTableFieldDescription
{
    private final String name;
    private final Type type;
    private final String mapping;
    private final String comment;
    private final String dataFormat;
    private final String formatHint;
    private final boolean hidden;

    @JsonCreator
    public RedisTableFieldDescription(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("comment") String comment,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("hidden") boolean hidden)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.comment = comment;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.hidden = hidden;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    RedisColumnHandle getColumnHandle(String connectorId, boolean keyDecoder, int index)
    {
        return new RedisColumnHandle(
                connectorId,
                index,
                getName(),
                getType(),
                getMapping(),
                getDataFormat(),
                getFormatHint(),
                keyDecoder,
                isHidden(),
                false);
    }

    ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(getName(), getType(), getComment(), isHidden());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, mapping, dataFormat, formatHint, hidden);
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

        RedisTableFieldDescription other = (RedisTableFieldDescription) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.mapping, other.mapping) &&
                Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.formatHint, other.formatHint) &&
                Objects.equals(this.hidden, other.hidden);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .add("hidden", hidden)
                .toString();
    }
}
