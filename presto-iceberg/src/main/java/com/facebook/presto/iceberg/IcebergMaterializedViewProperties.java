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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.MaterializedViewRefreshType;
import com.facebook.presto.spi.MaterializedViewStaleReadBehavior;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.durationProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Properties specific to Iceberg materialized views.
 * This class provides the property definitions and accessor methods
 * for materialized view-specific properties. It combines the base table
 * properties (needed for the storage table) with MV-specific properties.
 */
public class IcebergMaterializedViewProperties
{
    public static final String STORAGE_SCHEMA = "storage_schema";
    public static final String STORAGE_TABLE = "storage_table";
    public static final String STALE_READ_BEHAVIOR = "stale_read_behavior";
    public static final String STALENESS_WINDOW = "staleness_window";
    public static final String REFRESH_TYPE = "refresh_type";
    public static final String MAX_SNAPSHOTS_PER_REFRESH = "max_snapshots_per_refresh";

    private final List<PropertyMetadata<?>> materializedViewProperties;

    @Inject
    public IcebergMaterializedViewProperties(IcebergTableProperties tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");

        // MV-specific properties
        List<PropertyMetadata<?>> mvOnlyProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        STORAGE_SCHEMA,
                        "Schema for the materialized view storage table (defaults to same schema as the materialized view)",
                        null,
                        true))
                .add(stringProperty(
                        STORAGE_TABLE,
                        "Custom name for the materialized view storage table (defaults to generated name)",
                        null,
                        true))
                .add(new PropertyMetadata<>(
                        STALE_READ_BEHAVIOR,
                        "Behavior when reading from a stale materialized view (FAIL or USE_VIEW_QUERY)",
                        createUnboundedVarcharType(),
                        MaterializedViewStaleReadBehavior.class,
                        null,
                        true,
                        value -> value == null ? null : MaterializedViewStaleReadBehavior.valueOf(((String) value).toUpperCase(ENGLISH)),
                        value -> value == null ? null : ((MaterializedViewStaleReadBehavior) value).name()))
                .add(durationProperty(
                        STALENESS_WINDOW,
                        "Staleness window for materialized view (e.g., '1h', '30m', '0s')",
                        null,
                        true))
                .add(new PropertyMetadata<>(
                        REFRESH_TYPE,
                        "Refresh type for materialized view (FULL or INCREMENTAL)",
                        createUnboundedVarcharType(),
                        MaterializedViewRefreshType.class,
                        null,
                        true,
                        value -> value == null ? null : MaterializedViewRefreshType.valueOf(((String) value).toUpperCase(ENGLISH)),
                        value -> value == null ? null : ((MaterializedViewRefreshType) value).name()))
                .add(new PropertyMetadata<>(
                        MAX_SNAPSHOTS_PER_REFRESH,
                        "Maximum number of snapshots consumed per base table per refresh. " +
                                "Unset falls back to the session default.",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> {
                            if (value == null) {
                                return null;
                            }
                            int parsed = ((Number) value).intValue();
                            if (parsed <= 0) {
                                throw new PrestoException(INVALID_TABLE_PROPERTY,
                                        format("%s must be positive, got %d", MAX_SNAPSHOTS_PER_REFRESH, parsed));
                            }
                            return parsed;
                        },
                        object -> object))
                .build();

        // Combine table properties (for storage table) with MV-specific properties
        materializedViewProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(tableProperties.getTableProperties())
                .addAll(mvOnlyProperties)
                .build();
    }

    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    public static Optional<String> getStorageSchema(Map<String, Object> properties)
    {
        return Optional.ofNullable((String) properties.get(STORAGE_SCHEMA));
    }

    public static Optional<String> getStorageTable(Map<String, Object> properties)
    {
        return Optional.ofNullable((String) properties.get(STORAGE_TABLE));
    }

    public static Optional<MaterializedViewStaleReadBehavior> getStaleReadBehavior(Map<String, Object> properties)
    {
        return Optional.ofNullable((MaterializedViewStaleReadBehavior) properties.get(STALE_READ_BEHAVIOR));
    }

    public static Optional<Duration> getStalenessWindow(Map<String, Object> properties)
    {
        return Optional.ofNullable((Duration) properties.get(STALENESS_WINDOW));
    }

    public static Optional<MaterializedViewRefreshType> getRefreshType(Map<String, Object> properties)
    {
        return Optional.ofNullable((MaterializedViewRefreshType) properties.get(REFRESH_TYPE));
    }

    public static OptionalInt getMaxSnapshotsPerRefresh(Map<String, Object> properties)
    {
        Integer value = (Integer) properties.get(MAX_SNAPSHOTS_PER_REFRESH);
        return value == null ? OptionalInt.empty() : OptionalInt.of(value);
    }
}
