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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Types;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class ClickHouseColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final ClickHouseTypeHandle clickHouseTypeHandle;
    private final Type columnType;
    private final boolean nullable;

    public ClickHouseColumnType getType()
    {
        return type;
    }

    private final ClickHouseColumnType type;

    public ClickHouseColumnHandle(
            String connectorId,
            String columnName,
            ClickHouseTypeHandle clickHouseTypeHandle,
            Type columnType,
            boolean nullable)
    {
        this(connectorId, columnName, clickHouseTypeHandle, columnType, nullable, ClickHouseColumnType.REGULAR);
    }

    public ClickHouseColumnHandle(
            VariableReferenceExpression variable,
            ClickHouseColumnType type)
    {
        this.connectorId = null;
        this.columnName = requireNonNull(variable.getName(), "columnName is null");
        this.clickHouseTypeHandle = requireNonNull(toMappingDefaultJdbcHandle(variable.getType()), "clickHouseTypeHandle is null");
        this.columnType = requireNonNull(variable.getType(), "columnType is null");
        this.nullable = true;
        this.type = type;
    }

    public ClickHouseColumnHandle(
            String columnName,
            Type columnType,
            ClickHouseColumnType type)
    {
        this.connectorId = null;
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.clickHouseTypeHandle = requireNonNull(toMappingDefaultJdbcHandle(columnType), "clickHouseTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = true;
        this.type = type;
    }

    @JsonCreator
    public ClickHouseColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("clickHouseTypeHandle") ClickHouseTypeHandle clickHouseTypeHandle,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("type") ClickHouseColumnType type)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.clickHouseTypeHandle = requireNonNull(clickHouseTypeHandle, "clickHouseTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
        this.type = type;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public ClickHouseTypeHandle getClickHouseTypeHandle()
    {
        return clickHouseTypeHandle;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, nullable, null, null, false, emptyMap());
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
        ClickHouseColumnHandle o = (ClickHouseColumnHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("clickHouseTypeHandle", clickHouseTypeHandle)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .add("type", type)
                .toString();
    }

    public enum ClickHouseColumnType
    {
        REGULAR,
        DERIVED,
    }

    public static ClickHouseTypeHandle toMappingDefaultJdbcHandle(Type type)
    {
        if (type == BOOLEAN) {
            return new ClickHouseTypeHandle(Types.BOOLEAN, Optional.of("boolean"), 1, 0, Optional.empty(), Optional.empty());
        }
        if (type.getTypeSignature().getBase().equals("row")) {
            return new ClickHouseTypeHandle(Types.DOUBLE, Optional.of("double precision"), 32, 4, Optional.empty(), Optional.empty());
        }
        if (type == TINYINT) {
            return new ClickHouseTypeHandle(Types.TINYINT, Optional.of("tinyint"), 2, 0, Optional.empty(), Optional.empty());
        }
        if (type == SMALLINT) {
            return new ClickHouseTypeHandle(Types.SMALLINT, Optional.of("smallint"), 1, 0, Optional.empty(), Optional.empty());
        }
        if (type == INTEGER) {
            return new ClickHouseTypeHandle(Types.INTEGER, Optional.of("integer"), 4, 0, Optional.empty(), Optional.empty());
        }
        if (type == BIGINT) {
            return new ClickHouseTypeHandle(Types.BIGINT, Optional.of("bigint"), 8, 0, Optional.empty(), Optional.empty());
        }
        if (type == REAL) {
            return new ClickHouseTypeHandle(Types.REAL, Optional.of("real"), 16, 4, Optional.empty(), Optional.empty());
        }
        if (type == DOUBLE) {
            return new ClickHouseTypeHandle(Types.DOUBLE, Optional.of("double precision"), 32, 4, Optional.empty(), Optional.empty());
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return new ClickHouseTypeHandle(Types.VARCHAR, Optional.of("String"), 100, 0, Optional.empty(), Optional.empty());
        }
        if (type instanceof VarbinaryType) {
            return new ClickHouseTypeHandle(Types.VARCHAR, Optional.of("String"), 2000, 0, Optional.empty(), Optional.empty());
        }
        if (type == DATE) {
            return new ClickHouseTypeHandle(Types.DATE, Optional.of("date"), 8, 0, Optional.empty(), Optional.empty());
        }
        if (type == TIME) {
            return new ClickHouseTypeHandle(Types.TIME, Optional.of("time"), 4, 0, Optional.empty(), Optional.empty());
        }
        if (type == TIMESTAMP) {
            return new ClickHouseTypeHandle(Types.TIMESTAMP, Optional.of("timestamp"), 8, 0, Optional.empty(), Optional.empty());
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }
}
