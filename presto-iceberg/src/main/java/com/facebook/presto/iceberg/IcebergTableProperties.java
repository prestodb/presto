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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.iceberg.IcebergWarningCode.USE_OF_DEPRECATED_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;

public class IcebergTableProperties
{
    /**
     * Please use  {@link TableProperties#DEFAULT_FILE_FORMAT}
     */
    @Deprecated
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";
    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String LOCATION_PROPERTY = "location";

    /**
     * Please use  {@link TableProperties#FORMAT_VERSION}
     */
    @Deprecated
    public static final String FORMAT_VERSION = "format_version";
    /**
     * Please use  {@link TableProperties#COMMIT_NUM_RETRIES}
     */
    @Deprecated
    public static final String COMMIT_RETRIES = "commit_retries";
    /**
     * Please use  {@link TableProperties#DELETE_MODE}
     */
    @Deprecated
    public static final String DELETE_MODE = "delete_mode";
    /**
     * Please use  {@link TableProperties#METADATA_PREVIOUS_VERSIONS_MAX}
     */
    @Deprecated
    public static final String METADATA_PREVIOUS_VERSIONS_MAX = "metadata_previous_versions_max";
    /**
     * Please use  {@link TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED}
     */
    @Deprecated
    public static final String METADATA_DELETE_AFTER_COMMIT = "metadata_delete_after_commit";
    /**
     * Please use  {@link TableProperties#METRICS_MAX_INFERRED_COLUMN_DEFAULTS}
     */
    @Deprecated
    public static final String METRICS_MAX_INFERRED_COLUMN = "metrics_max_inferred_column";
    public static final String TARGET_SPLIT_SIZE = TableProperties.SPLIT_SIZE;

    private static final String DEPRECATION_WARNING_MESSAGE = "The table property \"%s\" has been " +
            "deprecated. Please migrate to using \"%s\". This will become an error in future versions";

    private static final BiMap<String, String> DEPRECATED_PROPERTIES = ImmutableBiMap.<String, String>builder()
            .put(FILE_FORMAT_PROPERTY, TableProperties.DEFAULT_FILE_FORMAT)
            .put(FORMAT_VERSION, TableProperties.FORMAT_VERSION)
            .put(COMMIT_RETRIES, TableProperties.COMMIT_NUM_RETRIES)
            .put(DELETE_MODE, TableProperties.DELETE_MODE)
            .put(METADATA_PREVIOUS_VERSIONS_MAX, TableProperties.METADATA_PREVIOUS_VERSIONS_MAX)
            .put(METADATA_DELETE_AFTER_COMMIT, TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED)
            .put(METRICS_MAX_INFERRED_COLUMN, TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS)
            .build();

    private static final Set<String> UPDATABLE_PROPERTIES = ImmutableSet.<String>builder()
            .add(COMMIT_RETRIES)
            .add(COMMIT_NUM_RETRIES)
            .add(TARGET_SPLIT_SIZE)
            .build();

    private static final String DEFAULT_FORMAT_VERSION = "2";

    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;
    private final Map<String, PropertyMetadata<?>> deprecatedPropertyMetadata;

    @Inject
    public IcebergTableProperties(IcebergConfig icebergConfig)
    {
        List<PropertyMetadata<?>> properties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "Partition transforms",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        SORTED_BY_PROPERTY,
                        "Sorted columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> (List<?>) value,
                        value -> value))
                .add(new PropertyMetadata<>(
                        TableProperties.DEFAULT_FILE_FORMAT,
                        "File format for the table",
                        createUnboundedVarcharType(),
                        FileFormat.class,
                        icebergConfig.getFileFormat(),
                        false,
                        value -> FileFormat.valueOf(((String) value).toUpperCase(ENGLISH)),
                        Enum::toString))
                .add(stringProperty(
                        WRITE_DATA_LOCATION,
                        "File system location URI for the table's data and delete files",
                        null,
                        false))
                .add(stringProperty(
                        TableProperties.FORMAT_VERSION,
                        "Format version for the table",
                        DEFAULT_FORMAT_VERSION,
                        false))
                .add(integerProperty(
                        TableProperties.COMMIT_NUM_RETRIES,
                        "Determines the number of attempts in case of concurrent upserts and deletes",
                        TableProperties.COMMIT_NUM_RETRIES_DEFAULT,
                        false))
                .add(new PropertyMetadata<>(
                        TableProperties.DELETE_MODE,
                        "Delete mode for the table",
                        createUnboundedVarcharType(),
                        RowLevelOperationMode.class,
                        RowLevelOperationMode.MERGE_ON_READ,
                        false,
                        value -> RowLevelOperationMode.fromName((String) value),
                        RowLevelOperationMode::modeName))
                .add(integerProperty(
                        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                        "The max number of old metadata files to keep in metadata log",
                        icebergConfig.getMetadataPreviousVersionsMax(),
                        false))
                .add(booleanProperty(
                        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                        "Whether enables to delete the oldest metadata file after commit",
                        icebergConfig.isMetadataDeleteAfterCommit(),
                        false))
                .add(integerProperty(
                        TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS,
                        "The maximum number of columns for which metrics are collected",
                        icebergConfig.getMetricsMaxInferredColumn(),
                        false))
                .add(new PropertyMetadata<>(
                        UPDATE_MODE,
                        "Update mode for the table",
                        createUnboundedVarcharType(),
                        RowLevelOperationMode.class,
                        RowLevelOperationMode.MERGE_ON_READ,
                        false,
                        value -> RowLevelOperationMode.fromName((String) value),
                        RowLevelOperationMode::modeName))
                .add(longProperty(TARGET_SPLIT_SIZE,
                        "Desired size of split to generate during query scan planning",
                        TableProperties.SPLIT_SIZE_DEFAULT,
                        false))
                .build();

        deprecatedPropertyMetadata = properties.stream()
                .filter(prop -> DEPRECATED_PROPERTIES.inverse().containsKey(prop.getName()))
                .map(prop -> new PropertyMetadata<>(
                        DEPRECATED_PROPERTIES.inverse().get(prop.getName()),
                        prop.getDescription(),
                        prop.getSqlType(),
                        (Class<Object>) prop.getJavaType(),
                        prop.getDefaultValue(),
                        true,
                        (Function<Object, Object>) prop.getDecoder(),
                        (Function<Object, Object>) prop.getEncoder()))
                .collect(toImmutableMap(property -> property.getName(), property -> property));

        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(properties)
                .addAll(deprecatedPropertyMetadata.values().iterator())
                .build();

        columnProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITIONING_PROPERTY,
                        "This column's partition transforms, supports both string expressions (e.g., 'bucket(4)') and array expressions (e.g. ARRAY['bucket(4)', 'identity'])",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value)
                        .withAdditionalTypeHandler(VARCHAR, ImmutableList::of));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static Set<String> getUpdatableProperties()
    {
        return UPDATABLE_PROPERTIES;
    }

    /**
     * @return a map of deprecated property name to new property name, or null if the property is
     * removed entirely.
     */
    public static Map<String, String> getDeprecatedProperties()
    {
        return DEPRECATED_PROPERTIES;
    }

    public FileFormat getFileFormat(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (FileFormat) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.DEFAULT_FILE_FORMAT);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitioning(Map<String, Object> tableProperties)
    {
        List<String> partitioning = (List<String>) tableProperties.get(PARTITIONING_PROPERTY);
        return partitioning == null ? ImmutableList.of() : ImmutableList.copyOf(partitioning);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getSortOrder(Map<String, Object> tableProperties)
    {
        List<String> sortedBy = (List<String>) tableProperties.get(SORTED_BY_PROPERTY);
        return sortedBy == null ? ImmutableList.of() : ImmutableList.copyOf(sortedBy);
    }

    public static String getTableLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(LOCATION_PROPERTY);
    }

    public static String getWriteDataLocation(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(WRITE_DATA_LOCATION);
    }

    public String getFormatVersion(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (String) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.FORMAT_VERSION);
    }

    public Integer getCommitRetries(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (Integer) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.COMMIT_NUM_RETRIES);
    }

    public RowLevelOperationMode getDeleteMode(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (RowLevelOperationMode) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.DELETE_MODE);
    }

    public Integer getMetadataPreviousVersionsMax(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (Integer) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.METADATA_PREVIOUS_VERSIONS_MAX);
    }

    public Boolean isMetadataDeleteAfterCommit(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (Boolean) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED);
    }

    public Integer getMetricsMaxInferredColumn(ConnectorSession session, Map<String, Object> tableProperties)
    {
        return (Integer) getTablePropertyWithDeprecationWarning(session, tableProperties, TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS);
    }

    public RowLevelOperationMode getUpdateMode(Map<String, Object> tableProperties)
    {
        return (RowLevelOperationMode) tableProperties.get(TableProperties.UPDATE_MODE);
    }

    public static Long getTargetSplitSize(Map<String, Object> tableProperties)
    {
        return (Long) tableProperties.get(TableProperties.SPLIT_SIZE);
    }

    @VisibleForTesting
    protected Object getTablePropertyWithDeprecationWarning(ConnectorSession session, Map<String, Object> tableProperties, String keyName)
    {
        String deprecatedProperty = DEPRECATED_PROPERTIES.inverse().get(keyName);
        if (deprecatedProperty == null) {
            return tableProperties.get(keyName);
        }

        // If the deprecated property returns the default value, the user may not have set it
        // intentionally. Return the value using the newer key name. This is OK because the
        // deprecated property names have the same defaults as the new names.
        PropertyMetadata<?> deprecatedPropertyData = deprecatedPropertyMetadata.get(deprecatedProperty);
        if (!tableProperties.containsKey(deprecatedProperty) || deprecatedPropertyData.getDefaultValue().equals(tableProperties.get(deprecatedProperty))) {
            return tableProperties.get(keyName);
        }

        // deprecated property did not use the default value, warn the user about the deprecation
        Object value = tableProperties.get(deprecatedProperty);
        session.getWarningCollector()
                .add(new PrestoWarning(
                        USE_OF_DEPRECATED_TABLE_PROPERTY,
                        format(DEPRECATION_WARNING_MESSAGE, deprecatedProperty, keyName)));
        return value;
    }
}
