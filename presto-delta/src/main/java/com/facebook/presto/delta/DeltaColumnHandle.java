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
import com.facebook.presto.spi.TableFormatColumnHandle;
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
        implements TableFormatColumnHandle
{
    private final String name;
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
            @JsonProperty("columnName") String name,
            @JsonProperty("dataType") TypeSignature dataType,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("subfield") Optional<Subfield> subfield)
    {
        this.name = requireNonNull(name, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.subfield = requireNonNull(subfield, "subfield is null");
    }

    @JsonProperty
    @Override
    public String getName()
    {
        return name;
    }

    @JsonProperty
    @Override
    public String getBaseType()
    {
        throw new UnsupportedOperationException();
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
                .add("name", name)
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
        return name.equals(that.name) &&
                dataType.equals(that.dataType) &&
                columnType == that.columnType &&
                subfield.equals(that.subfield);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, dataType, columnType, subfield);
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
