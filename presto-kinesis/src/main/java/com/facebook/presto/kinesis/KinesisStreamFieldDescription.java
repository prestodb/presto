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
package com.facebook.presto.kinesis;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class KinesisStreamFieldDescription
{
    private final String name;
    private final Type type;
    private final String mapping;
    private final String comment;
    private final String dataFormat;
    private final String formatHint;
    private final boolean hidden;

    @JsonCreator
    public KinesisStreamFieldDescription(
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
        this.type = checkNotNull(type, "type is null");
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

    KinesisColumnHandle getColumnHandle(String connectorId, int index)
    {
        return new KinesisColumnHandle(connectorId,
                index,
                getName(),
                getType(),
                getMapping(),
                getDataFormat(),
                getFormatHint(),
                isHidden(),
                false);
    }

    ColumnMetadata getColumnMetadata(int index)
    {
        return new ColumnMetadata(getName(), getType(), index, false, getComment(), isHidden());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type, mapping, dataFormat, formatHint, hidden);
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

        KinesisStreamFieldDescription other = (KinesisStreamFieldDescription) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type) &&
                Objects.equal(this.mapping, other.mapping) &&
                Objects.equal(this.dataFormat, other.dataFormat) &&
                Objects.equal(this.formatHint, other.formatHint) &&
                Objects.equal(this.hidden, other.hidden);
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
