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
package io.prestosql.decoder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Test column handle for decoders.
 */
public final class DecoderTestColumnHandle
        implements DecoderColumnHandle, Comparable<DecoderTestColumnHandle>
{
    private final int ordinalPosition;

    /**
     * Column Name
     */
    private final String name;

    /**
     * Column type
     */
    private final Type type;

    /**
     * Mapping hint for the decoder
     */
    private final String mapping;

    /**
     * Data format to use (selects the decoder)
     */
    private final String dataFormat;

    /**
     * Additional format hint for the selected decoder. Selects a decoder subtype (e.g. which timestamp decoder).
     */
    private final String formatHint;

    /**
     * True if the key decoder should be used, false if the message decoder should be used.
     */
    private final boolean keyDecoder;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a table definition.
     */
    private final boolean internal;

    @JsonCreator
    public DecoderTestColumnHandle(
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("keyDecoder") boolean keyDecoder,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal)
    {
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.keyDecoder = keyDecoder;
        this.hidden = hidden;
        this.internal = internal;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
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

    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ordinalPosition, name, type, mapping, dataFormat, formatHint, keyDecoder, hidden, internal);
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

        DecoderTestColumnHandle other = (DecoderTestColumnHandle) obj;
        return Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.mapping, other.mapping) &&
                Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.formatHint, other.formatHint) &&
                Objects.equals(this.keyDecoder, other.keyDecoder) &&
                Objects.equals(this.hidden, other.hidden) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public int compareTo(DecoderTestColumnHandle otherHandle)
    {
        return Integer.compare(this.getOrdinalPosition(), otherHandle.getOrdinalPosition());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("ordinalPosition", ordinalPosition)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .add("keyDecoder", keyDecoder)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }
}
