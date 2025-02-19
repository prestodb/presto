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
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.longProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;

public class IcebergTableProperties
{
    public static final String FILE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONING_PROPERTY = "partitioning";

    public static final String SORTED_BY_PROPERTY = "sorted_by";
    public static final String LOCATION_PROPERTY = "location";
    public static final String FORMAT_VERSION = "format_version";
    public static final String COMMIT_RETRIES = "commit_retries";
    public static final String DELETE_MODE = "delete_mode";
    public static final String METADATA_PREVIOUS_VERSIONS_MAX = "metadata_previous_versions_max";
    public static final String METADATA_DELETE_AFTER_COMMIT = "metadata_delete_after_commit";
    public static final String METRICS_MAX_INFERRED_COLUMN = "metrics_max_inferred_column";
    public static final String TARGET_SPLIT_SIZE = TableProperties.SPLIT_SIZE;
    private static final String DEFAULT_FORMAT_VERSION = "2";

    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public IcebergTableProperties(IcebergConfig icebergConfig)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(new PropertyMetadata<>(
                        FILE_FORMAT_PROPERTY,
                        "File format for the table",
                        createUnboundedVarcharType(),
                        FileFormat.class,
                        icebergConfig.getFileFormat(),
                        false,
                        value -> FileFormat.valueOf(((String) value).toUpperCase(ENGLISH)),
                        value -> value.toString()))
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
                .add(stringProperty(
                        WRITE_DATA_LOCATION,
                        "File system location URI for the table's data and delete files",
                        null,
                        false))
                .add(stringProperty(
                        FORMAT_VERSION,
                        "Format version for the table",
                        DEFAULT_FORMAT_VERSION,
                        false))
                .add(integerProperty(
                        COMMIT_RETRIES,
                        "Determines the number of attempts in case of concurrent upserts and deletes",
                        TableProperties.COMMIT_NUM_RETRIES_DEFAULT,
                        false))
                .add(new PropertyMetadata<>(
                        DELETE_MODE,
                        "Delete mode for the table",
                        createUnboundedVarcharType(),
                        RowLevelOperationMode.class,
                        RowLevelOperationMode.MERGE_ON_READ,
                        false,
                        value -> RowLevelOperationMode.fromName((String) value),
                        RowLevelOperationMode::modeName))
                .add(integerProperty(
                        METADATA_PREVIOUS_VERSIONS_MAX,
                        "The max number of old metadata files to keep in metadata log",
                        icebergConfig.getMetadataPreviousVersionsMax(),
                        false))
                .add(booleanProperty(
                        METADATA_DELETE_AFTER_COMMIT,
                        "Whether enables to delete the oldest metadata file after commit",
                        icebergConfig.isMetadataDeleteAfterCommit(),
                        false))
                .add(integerProperty(
                        METRICS_MAX_INFERRED_COLUMN,
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

        columnProperties = ImmutableList.of(stringProperty(
                PARTITIONING_PROPERTY,
                "This column's partition transform",
                null,
                false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static FileFormat getFileFormat(Map<String, Object> tableProperties)
    {
        return (FileFormat) tableProperties.get(FILE_FORMAT_PROPERTY);
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

    public static String getFormatVersion(Map<String, Object> tableProperties)
    {
        return (String) tableProperties.get(FORMAT_VERSION);
    }

    public static Integer getCommitRetries(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(COMMIT_RETRIES);
    }

    public static RowLevelOperationMode getDeleteMode(Map<String, Object> tableProperties)
    {
        return (RowLevelOperationMode) tableProperties.get(DELETE_MODE);
    }

    public static Integer getMetadataPreviousVersionsMax(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(METADATA_PREVIOUS_VERSIONS_MAX);
    }

    public static Boolean isMetadataDeleteAfterCommit(Map<String, Object> tableProperties)
    {
        return (Boolean) tableProperties.get(METADATA_DELETE_AFTER_COMMIT);
    }

    public static Integer getMetricsMaxInferredColumn(Map<String, Object> tableProperties)
    {
        return (Integer) tableProperties.get(METRICS_MAX_INFERRED_COLUMN);
    }

    public static RowLevelOperationMode getUpdateMode(Map<String, Object> tableProperties)
    {
        return (RowLevelOperationMode) tableProperties.get(UPDATE_MODE);
    }

    public static Long getTargetSplitSize(Map<String, Object> tableProperties)
    {
        return (Long) tableProperties.get(TableProperties.SPLIT_SIZE);
    }
}
