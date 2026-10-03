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
package com.facebook.presto.ducklake;

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.ducklake.DuckLakeTableType.DATA;
import static com.facebook.presto.ducklake.DuckLakeTableType.SNAPSHOTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

/**
 * Parses a table name as it appears in a query (for example {@code orders} or {@code
 * orders$snapshots}) into the base table name and the requested {@link DuckLakeTableType}.
 * Mirrors Iceberg's {@code IcebergTableName}, but DuckLake does not support Iceberg's {@code
 * @version} or {@code .branch_} syntax; time travel is expressed with {@code FOR SYSTEM_VERSION
 * AS OF} instead. {@code snapshotId} is the snapshot resolved by the metadata layer (empty until
 * then); {@code from} never populates it.
 */
public class DuckLakeTableName
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("(?<table>[^$]+)(?:\\$(?<type>[^$]+))?");

    private final String tableName;
    private final DuckLakeTableType tableType;
    private final Optional<Long> snapshotId;
    private final boolean snapshotSpecified;

    @JsonCreator
    public DuckLakeTableName(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") DuckLakeTableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("snapshotSpecified") boolean snapshotSpecified)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.snapshotSpecified = snapshotSpecified;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public DuckLakeTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public boolean isSnapshotSpecified()
    {
        return snapshotSpecified;
    }

    public String getTableNameWithType()
    {
        if (tableType == DATA) {
            return tableName;
        }
        return tableName + "$" + tableType.name().toLowerCase(ROOT);
    }

    public DuckLakeTableName withSnapshotId(long snapshotId, boolean snapshotSpecified)
    {
        return new DuckLakeTableName(tableName, tableType, Optional.of(snapshotId), snapshotSpecified);
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
        DuckLakeTableName that = (DuckLakeTableName) o;
        return snapshotSpecified == that.snapshotSpecified &&
                tableName.equals(that.tableName) &&
                tableType == that.tableType &&
                snapshotId.equals(that.snapshotId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tableType, snapshotId, snapshotSpecified);
    }

    @Override
    public String toString()
    {
        return getTableNameWithType() + snapshotId.map(snapshot -> "@" + snapshot).orElse("");
    }

    /**
     * Parses a table name as it appears in a query. Only the {@code snapshots} suffix is
     * recognized; any other {@code $}-suffix (including a missing or repeated one) is rejected.
     */
    public static DuckLakeTableName from(String name)
    {
        requireNonNull(name, "name is null");
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new PrestoException(NOT_SUPPORTED, "Invalid DuckLake table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");

        DuckLakeTableType type = DATA;
        if (typeString != null) {
            if (!typeString.equalsIgnoreCase("snapshots")) {
                throw new PrestoException(NOT_SUPPORTED, format("Invalid DuckLake table name (unknown type '%s'): %s", typeString, name));
            }
            type = SNAPSHOTS;
        }

        return new DuckLakeTableName(table, type, Optional.empty(), false);
    }
}
