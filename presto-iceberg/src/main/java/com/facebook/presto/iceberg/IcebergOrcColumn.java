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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveColumnHandle.ColumnType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IcebergOrcColumn
{
    public static final int ROOT_COLUMN_ID = 0;

    private int orcColumnId;
    private int orcFieldTypeIndex;
    private Optional<Integer> icebergColumnId;
    private String columnName;
    private ColumnType columnType;
    private OrcTypeKind orcType;
    private Map<String, String> attributes;

    public IcebergOrcColumn(
            int orcColumnId,
            int orcFieldTypeIndex,
            Optional<Integer> icebergColumnId,
            String columnName,
            ColumnType columnType,
            OrcTypeKind orcType,
            Map<String, String> attributes)
    {
        checkArgument(orcColumnId >= 0, "orcColumnId is negative");
        checkArgument(orcFieldTypeIndex >= 0, "orcFieldTypeIndex is negative");
        this.orcColumnId = orcColumnId;
        this.orcFieldTypeIndex = orcFieldTypeIndex;
        this.icebergColumnId = requireNonNull(icebergColumnId, "icebergColumnId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.orcType = requireNonNull(orcType, "orcType is null");

        this.attributes = ImmutableMap.copyOf(requireNonNull(attributes, "attributes is null"));
    }

    public int getOrcColumnId()
    {
        return orcColumnId;
    }

    public int getOrcFieldTypeIndex()
    {
        return orcFieldTypeIndex;
    }

    public Optional<Integer> getIcebergColumnId()
    {
        return icebergColumnId;
    }

    public IcebergOrcColumn setIcebergColumnId(Optional<Integer> icebergColumnId)
    {
        this.icebergColumnId = requireNonNull(icebergColumnId, "icebergColumnId is null");
        return this;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public IcebergOrcColumn setColumnName(String columnName)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        return this;
    }

    public ColumnType getColumnType()
    {
        return columnType;
    }

    public IcebergOrcColumn setColumnType(ColumnType columnType)
    {
        this.columnType = requireNonNull(columnType, "columnType is null");
        return this;
    }

    public OrcTypeKind getOrcType()
    {
        return orcType;
    }

    public IcebergOrcColumn setOrcType(OrcTypeKind orcType)
    {
        this.orcType = requireNonNull(orcType, "orcType is null");
        return this;
    }

    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    public static IcebergOrcColumn copy(IcebergOrcColumn other)
    {
        requireNonNull(other, "copy from other IcebergOrcColumn is null");
        return new IcebergOrcColumn(
                other.getOrcColumnId(),
                other.getOrcFieldTypeIndex(),
                other.getIcebergColumnId(),
                other.getColumnName(),
                other.getColumnType(),
                other.getOrcType(),
                other.getAttributes());
    }

    @Override
    public String toString()
    {
        return "IcebergOrcColumn{" +
                "orcColumnId=" + orcColumnId +
                ", orcFieldTypeIndex=" + orcFieldTypeIndex +
                ", icebergColumnId=" + icebergColumnId +
                ", columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", orcType=" + orcType +
                ", attributes=" + attributes +
                '}';
    }
}
