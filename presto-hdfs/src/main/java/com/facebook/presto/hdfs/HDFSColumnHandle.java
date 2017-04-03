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
package com.facebook.presto.hdfs;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSColumnHandle
implements ColumnHandle
{
    public enum ColumnType
    {
        FIBER_COL,
        TIME_COL,
        REGULAR,
        NOTVALID
    }

    private final String name;
    private final Type type;
    private final String comment;
    private final ColumnType colType;
    private final String connectorId;

    @JsonCreator
    public HDFSColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("comment") String comment,
            @JsonProperty("colType") ColumnType colType,
            @JsonProperty("connectorId") String connectorId)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.colType = requireNonNull(colType, "col type is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
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
    public String getComment()
    {
        return comment;
    }

    @JsonProperty
    public ColumnType getColType()
    {
        return colType;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, comment, colType, connectorId);
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

        HDFSColumnHandle other = (HDFSColumnHandle) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(type, other.type) &&
                Objects.equals(comment, other.comment) &&
                Objects.equals(colType, other.colType) &&
                Objects.equals(connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("comment", comment)
                .add("column type", colType)
                .add("connector id", connectorId)
                .toString();
    }
}
