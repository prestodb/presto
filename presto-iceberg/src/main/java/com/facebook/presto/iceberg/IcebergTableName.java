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

import com.facebook.presto.spi.PrestoException;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.iceberg.TableType.DATA;
import static com.facebook.presto.iceberg.TableType.FILES;
import static com.facebook.presto.iceberg.TableType.MANIFESTS;
import static com.facebook.presto.iceberg.TableType.PARTITIONS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class IcebergTableName
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:@(?<ver1>[0-9]+))?" +
            "(?:\\$(?<type>[^@]+)(?:@(?<ver2>[0-9]+))?)?");

    private final String tableName;
    private final TableType tableType;
    private final Optional<Long> snapshotId;

    public IcebergTableName(String tableName, TableType tableType, Optional<Long> snapshotId)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
    }

    public String getTableName()
    {
        return tableName;
    }

    public TableType getTableType()
    {
        return tableType;
    }

    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    public String getTableNameWithType()
    {
        return tableName + "$" + tableType.name().toLowerCase(ROOT);
    }

    @Override
    public String toString()
    {
        return getTableNameWithType() + "@" + snapshotId;
    }

    public static IcebergTableName from(String name)
    {
        Matcher match = TABLE_PATTERN.matcher(name);
        if (!match.matches()) {
            throw new PrestoException(NOT_SUPPORTED, "Invalid Iceberg table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");
        String version1 = match.group("ver1");
        String version2 = match.group("ver2");

        TableType type = DATA;
        if (typeString != null) {
            try {
                type = TableType.valueOf(typeString.toUpperCase(ROOT));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(NOT_SUPPORTED, format("Invalid Iceberg table name (unknown type '%s'): %s", typeString, name));
            }
        }

        Optional<Long> version = Optional.empty();
        if (type == DATA || type == PARTITIONS || type == MANIFESTS || type == FILES) {
            if (version1 != null && version2 != null) {
                throw new PrestoException(NOT_SUPPORTED, "Invalid Iceberg table name (cannot specify two @ versions): " + name);
            }
            if (version1 != null) {
                version = Optional.of(parseLong(version1));
            }
            else if (version2 != null) {
                version = Optional.of(parseLong(version2));
            }
        }
        else if (version1 != null || version2 != null) {
            throw new PrestoException(NOT_SUPPORTED, format("Invalid Iceberg table name (cannot use @ version with table type '%s'): %s", type, name));
        }

        return new IcebergTableName(table, type, version);
    }
}
