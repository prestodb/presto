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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PulsarColumnHandle implements ColumnHandle {

    private final String connectorId;

    /**
     * Column Name
     */
    private final String name;

    /**
     * Column type
     */
    private final Type type;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a topic definition.
     */
    private final boolean internal;

    private Integer positionIndex;

    @JsonCreator
    public PulsarColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal,
            @JsonProperty("positionIndex") Integer positionIndex) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.hidden = hidden;
        this.internal = internal;
        this.positionIndex = positionIndex;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public boolean isHidden() {
        return hidden;
    }

    @JsonProperty
    public boolean isInternal() {
        return internal;
    }

    @JsonProperty
    public Integer getPositionIndex() {
        return positionIndex;
    }


    ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(name, type, null, hidden);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, name, type, hidden, internal);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PulsarColumnHandle other = (PulsarColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.hidden, other.hidden) &&
                Objects.equals(this.internal, other.internal) &&
                Objects.equals(this.positionIndex, other.positionIndex);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("type", type)
                .add("hidden", hidden)
                .add("internal", internal)
                .add("positionIndex", positionIndex)
                .toString();
    }
}
