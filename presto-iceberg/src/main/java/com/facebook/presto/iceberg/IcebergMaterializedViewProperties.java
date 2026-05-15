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
import com.google.common.collect.Maps;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_REFRESH_TYPE;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STALENESS_WINDOW;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STALE_READ_BEHAVIOR;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_MATERIALIZED_VIEW_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
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

    private static final List<MaterializedViewProperty> MV_ONLY_PROPERTIES = ImmutableList.of(
            creationOnly(
                    stringProperty(
                            STORAGE_SCHEMA,
                            "Schema for the materialized view storage table (defaults to same schema as the materialized view)",
                            null,
                            true),
                    PRESTO_MATERIALIZED_VIEW_STORAGE_SCHEMA),
            creationOnly(
                    stringProperty(
                            STORAGE_TABLE,
                            "Custom name for the materialized view storage table (defaults to generated name)",
                            null,
                            true),
                    PRESTO_MATERIALIZED_VIEW_STORAGE_TABLE_NAME),
            updatable(
                    new PropertyMetadata<>(
                            STALE_READ_BEHAVIOR,
                            "Behavior when reading from a stale materialized view (FAIL or USE_VIEW_QUERY)",
                            createUnboundedVarcharType(),
                            MaterializedViewStaleReadBehavior.class,
                            null,
                            true,
                            value -> value == null ? null : MaterializedViewStaleReadBehavior.valueOf(((String) value).toUpperCase(ENGLISH)),
                            value -> value == null ? null : ((MaterializedViewStaleReadBehavior) value).name()),
                    PRESTO_MATERIALIZED_VIEW_STALE_READ_BEHAVIOR,
                    value -> ((MaterializedViewStaleReadBehavior) value).name()),
            updatable(
                    durationProperty(
                            STALENESS_WINDOW,
                            "Staleness window for materialized view (e.g., '1h', '30m', '0s')",
                            null,
                            true),
                    PRESTO_MATERIALIZED_VIEW_STALENESS_WINDOW,
                    value -> ((Duration) value).toString()),
            updatable(
                    new PropertyMetadata<>(
                            REFRESH_TYPE,
                            "Refresh type for materialized view (FULL or INCREMENTAL)",
                            createUnboundedVarcharType(),
                            MaterializedViewRefreshType.class,
                            null,
                            true,
                            value -> value == null ? null : MaterializedViewRefreshType.valueOf(((String) value).toUpperCase(ENGLISH)),
                            value -> value == null ? null : ((MaterializedViewRefreshType) value).name()),
                    PRESTO_MATERIALIZED_VIEW_REFRESH_TYPE,
                    value -> ((MaterializedViewRefreshType) value).name()));

    private static final Map<String, MaterializedViewProperty> MV_ONLY_PROPERTIES_BY_NAME =
            Maps.uniqueIndex(MV_ONLY_PROPERTIES, property -> property.metadata().getName());

    private final List<PropertyMetadata<?>> materializedViewProperties;

    @Inject
    public IcebergMaterializedViewProperties(IcebergTableProperties tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");

        // Combine table properties (for storage table) with MV-specific properties
        materializedViewProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(tableProperties.getTableProperties())
                .addAll(MV_ONLY_PROPERTIES.stream().map(MaterializedViewProperty::metadata).iterator())
                .build();
    }

    public List<PropertyMetadata<?>> getMaterializedViewProperties()
    {
        return materializedViewProperties;
    }

    public static Map.Entry<String, String> serializeForUpdate(String name, Object value)
    {
        MaterializedViewProperty property = MV_ONLY_PROPERTIES_BY_NAME.get(name);
        if (property == null || !property.updatable()) {
            throw new PrestoException(INVALID_MATERIALIZED_VIEW_PROPERTY,
                    format("Materialized view property is not updatable: %s", name));
        }
        if (value == null) {
            throw new PrestoException(NOT_SUPPORTED,
                    format("Clearing materialized view property %s is not supported", name));
        }
        return Map.entry(property.storageKey(), property.serializer().apply(value));
    }

    private static MaterializedViewProperty creationOnly(PropertyMetadata<?> metadata, String storageKey)
    {
        return new MaterializedViewProperty(metadata, storageKey, null);
    }

    private static MaterializedViewProperty updatable(PropertyMetadata<?> metadata, String storageKey, Function<Object, String> serializer)
    {
        return new MaterializedViewProperty(metadata, storageKey, requireNonNull(serializer, "serializer is null"));
    }

    private static final class MaterializedViewProperty
    {
        private final PropertyMetadata<?> metadata;
        private final String storageKey;
        private final Function<Object, String> serializer;

        MaterializedViewProperty(PropertyMetadata<?> metadata, String storageKey, Function<Object, String> serializer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.storageKey = requireNonNull(storageKey, "storageKey is null");
            this.serializer = serializer;
        }

        PropertyMetadata<?> metadata()
        {
            return metadata;
        }

        String storageKey()
        {
            return storageKey;
        }

        Function<Object, String> serializer()
        {
            return serializer;
        }

        boolean updatable()
        {
            return serializer != null;
        }
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
}
