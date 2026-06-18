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
package com.facebook.presto.delta;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.SUBFIELD;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DeltaColumnHandle
        implements ColumnHandle
{
    private final Long id;
    private final String physicalName;
    private final String logicalName;
    private final TypeSignature dataType;
    private final ColumnType columnType;
    private final Optional<Subfield> subfield;

    public enum ColumnType
    {
        REGULAR,
        PARTITION,
        SUBFIELD,
    }

    @JsonCreator
    public DeltaColumnHandle(
            @JsonProperty("id") Long id,
            @JsonProperty("physicalName") String physicalName,
            @JsonProperty("columnName") String logicalName,
            @JsonProperty("dataType") TypeSignature dataType,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("subfield") Optional<Subfield> subfield)
    {
        this.id = id;
        this.physicalName = physicalName;
        this.logicalName = requireNonNull(logicalName, "logicalName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.subfield = requireNonNull(subfield, "subfield is null");
    }

    @JsonProperty
    public Long getId()
    {
        return this.id;
    }

    @JsonProperty
    public String getPhysicalName()
    {
        return this.physicalName;
    }

    @JsonProperty
    public String getLogicalName()
    {
        return this.logicalName;
    }

    /**
     * Returns the appropriate name based on the column mapping of the table
     * This will return the physical name if column mapping is enabled, the logical name otherwise
     * @return a {@link String} representing the name of the column
     */
    public String getSourceName()
    {
        return this.physicalName == null ? this.logicalName : this.physicalName;
    }

    @JsonProperty
    public TypeSignature getDataType()
    {
        return dataType;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<Subfield> getSubfield()
    {
        return subfield;
    }

    @Override
    public String toString()
    {
        ToStringHelper stringHelper = toStringHelper(this)
                .add("id", this.id)
                .add("physicalName", this.physicalName)
                .add("logicalName", this.logicalName)
                .add("dataType", dataType)
                .add("columnType", columnType);

        if (subfield.isPresent()) {
            stringHelper = stringHelper.add("subfield", subfield.get());
        }

        return stringHelper.toString();
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

        DeltaColumnHandle that = (DeltaColumnHandle) o;
        return Objects.equals(this.id, that.id) &&
                Objects.equals(this.physicalName, that.physicalName) &&
                this.logicalName.equals(that.logicalName) &&
                dataType.equals(that.dataType) &&
                columnType == that.columnType &&
                subfield.equals(that.subfield);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.id, this.physicalName, this.logicalName, dataType, columnType, subfield);
    }

    /**
     * Return the pushed down subfield if the column represents one
     */
    public static Subfield getPushedDownSubfield(DeltaColumnHandle column)
    {
        checkArgument(isPushedDownSubfield(column), format("not a valid pushed down subfield: %s", column));
        return column.getSubfield().get();
    }

    public static boolean isPushedDownSubfield(DeltaColumnHandle column)
    {
        return column.getColumnType() == SUBFIELD;
    }
}
