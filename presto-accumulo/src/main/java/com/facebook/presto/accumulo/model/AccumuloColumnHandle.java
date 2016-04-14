/*
 * Copyright 2016 Bloomberg L.P.
 *
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
package com.facebook.presto.accumulo.model;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Jackson object and an implementation of a Presto ColumnHandle. Encapsulates all data referring to
 * a column: name, Accumulo column mapping, type, ordinal, etc.
 */
public final class AccumuloColumnHandle
        implements ColumnHandle, Comparable<AccumuloColumnHandle>
{
    private final boolean indexed;
    private final String connectorId;
    private String name;
    private final String family;
    private final String qualifier;
    private final Type type;
    private int ordinal;
    private final String comment;

    /**
     * JSON Creator for a new {@link AccumuloColumnHandle} object
     *
     * @param connectorId Connector ID
     * @param name Presto column name
     * @param family Accumulo column family
     * @param qualifier Accumulo column qualifier
     * @param type Presto type
     * @param ordinal Ordinal of the column within the row
     * @param comment Comment for the column
     * @param indexed True if the column has entries in the index table, false otherwise
     */
    @JsonCreator
    public AccumuloColumnHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name, @JsonProperty("family") String family,
            @JsonProperty("qualifier") String qualifier, @JsonProperty("type") Type type,
            @JsonProperty("ordinal") int ordinal, @JsonProperty("comment") String comment,
            @JsonProperty("indexed") boolean indexed)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        this.family = family;
        this.qualifier = qualifier;
        this.type = requireNonNull(type, "type is null");
        this.ordinal = requireNonNull(ordinal, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.indexed = requireNonNull(indexed, "indexed is null");
    }

    /**
     * Gets the Presto connector ID.
     *
     * @return Connector ID
     */
    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    /**
     * Gets the Presto column name.
     *
     * @return Presto column name
     */
    @JsonProperty
    public String getName()
    {
        return name;
    }

    /**
     * Setter function for the column name
     * <p>
     * Added to this class for column rename support
     *
     * @param name New column name
     */
    @JsonSetter
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the Accumulo column family.
     *
     * @return Column family
     */
    @JsonProperty
    public String getFamily()
    {
        return family;
    }

    /**
     * Gets the Accumulo column qualifier.
     *
     * @return Column qualifier
     */
    @JsonProperty
    public String getQualifier()
    {
        return qualifier;
    }

    /**
     * Gets the Presto column type.
     *
     * @return Presto type
     */
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    /**
     * Gets the column ordinal within the row.
     *
     * @return Column ordinal
     */
    @JsonProperty
    public int getOrdinal()
    {
        return ordinal;
    }

    /**
     * Setter method for the ordinal (for support of adding new columns)
     *
     * @param ordinal New ordinal
     */
    @JsonSetter
    public void setOrdinal(int ordinal)
    {
        this.ordinal = ordinal;
    }

    /**
     * Gets the column comment.
     *
     * @return Comment
     */
    @JsonProperty
    public String getComment()
    {
        return comment;
    }

    /**
     * Gets a new {@link ColumnMetadata} regarding this column. This is ignored by Jackson.
     *
     * @return Column metadata
     */
    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        // TODO Partition key? Technically the row ID would be partitioned, not sure what this does
        // for Presto, though.
        return new ColumnMetadata(name, type, comment, false);
    }

    /**
     * Gets a Boolean value indicating whether or not this column contains entries in the index
     * table.
     *
     * @return True if indexed, false otherwise
     */
    @JsonProperty
    public boolean isIndexed()
    {
        return indexed;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexed, connectorId, name, family, qualifier, type, ordinal, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        AccumuloColumnHandle other = (AccumuloColumnHandle) obj;
        return Objects.equals(this.indexed, other.indexed)
                && Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.name, other.name)
                && Objects.equals(this.family, other.family)
                && Objects.equals(this.qualifier, other.qualifier)
                && Objects.equals(this.type, other.type)
                && Objects.equals(this.ordinal, other.ordinal)
                && Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).add("name", name)
                .add("columnFamily", family).add("columnQualifier", qualifier).add("type", type)
                .add("ordinal", ordinal).add("comment", comment).add("indexed", indexed).toString();
    }

    /**
     * Compares this column's ordinal against the given column's ordinal
     *
     * @return Comparison value of the column ordinals
     */
    @Override
    public int compareTo(AccumuloColumnHandle o)
    {
        return Integer.compare(this.getOrdinal(), o.getOrdinal());
    }
}
