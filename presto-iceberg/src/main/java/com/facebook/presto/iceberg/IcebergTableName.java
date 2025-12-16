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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.iceberg.IcebergTableType.CHANGELOG;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergTableType.FILES;
import static com.facebook.presto.iceberg.IcebergTableType.HISTORY;
import static com.facebook.presto.iceberg.IcebergTableType.MANIFESTS;
import static com.facebook.presto.iceberg.IcebergTableType.METADATA_LOG_ENTRIES;
import static com.facebook.presto.iceberg.IcebergTableType.PARTITIONS;
import static com.facebook.presto.iceberg.IcebergTableType.PROPERTIES;
import static com.facebook.presto.iceberg.IcebergTableType.REFS;
import static com.facebook.presto.iceberg.IcebergTableType.SNAPSHOTS;
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
    private final IcebergTableType icebergTableType;
    private final Optional<Long> snapshotId;

    private final Optional<Long> changelogEndSnapshot;

    private static final Set<IcebergTableType> SYSTEM_TABLES = Sets.immutableEnumSet(FILES, MANIFESTS, PARTITIONS, HISTORY, SNAPSHOTS, PROPERTIES, REFS, METADATA_LOG_ENTRIES);

    @JsonCreator
    public IcebergTableName(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") IcebergTableType icebergTableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("changelogEndSnapshot") Optional<Long> changelogEndSnapshot)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.icebergTableType = requireNonNull(icebergTableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.changelogEndSnapshot = requireNonNull(changelogEndSnapshot, "changelogEndSnapshot is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public IcebergTableType getTableType()
    {
        return icebergTableType;
    }

    @JsonProperty
    public Optional<Long> getChangelogEndSnapshot()
    {
        return changelogEndSnapshot;
    }

    public boolean isSystemTable()
    {
        return SYSTEM_TABLES.contains(icebergTableType);
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    public String getTableNameWithType()
    {
        return tableName + "$" + icebergTableType.name().toLowerCase(ROOT);
    }

    @Override
    public String toString()
    {
        return getTableNameWithType() + snapshotId.map(snap -> "@" + snap).orElse("");
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

        IcebergTableType type = DATA;
        if (typeString != null) {
            try {
                type = IcebergTableType.valueOf(typeString.toUpperCase(ROOT));
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(NOT_SUPPORTED, format("Invalid Iceberg table name (unknown type '%s'): %s", typeString, name));
            }
        }

        if (!type.isPublic()) {
            throw new PrestoException(NOT_SUPPORTED, format("Internal Iceberg table name (type '%s'): %s", typeString, name));
        }

        Optional<Long> version = Optional.empty();
        Optional<Long> changelogEndVersion = Optional.empty();
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
        else if (type == CHANGELOG) {
            version = Optional.ofNullable(version1).map(Long::parseLong);
            changelogEndVersion = Optional.ofNullable(version2).map(Long::parseLong);
        }
        else if (version1 != null || version2 != null) {
            throw new PrestoException(NOT_SUPPORTED, format("Invalid Iceberg table name (cannot use @ version with table type '%s'): %s", type, name));
        }

        return new IcebergTableName(table, type, version, changelogEndVersion);
    }
}
