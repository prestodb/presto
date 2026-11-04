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

import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Typed handle for DELETE table operations on Iceberg.
 *
 * <p>Previously, the Iceberg connector reused {@link IcebergTableHandle} as
 * its {@link ConnectorDeleteTableHandle} (registered via {@link
 * IcebergHandleResolver#getDeleteTableHandleClass()}). That works for V2
 * tables, where DELETE flows through a Java row-id-rewrite path that does
 * not need a dedicated handle. For V3 tables (deletion vectors / Puffin)
 * the native worker needs to recognize this handle as an Iceberg-specific
 * delete handle when converting the plan to a Velox {@code
 * IcebergInsertTableHandle} with {@code WriteKind::kDeletionVector}.
 *
 * <p>This class mirrors {@link IcebergInsertTableHandle} and adds a single
 * {@link #getFileContent()} field carrying the Iceberg content type
 * ({@code POSITION_DELETES} for V2, {@code DELETION_VECTOR} for V3). The
 * Prestissimo bridge derives the Velox {@code WriteKind} from this field.
 */
public class IcebergDeleteTableHandle
        extends IcebergWritableTableHandle
        implements ConnectorDeleteTableHandle
{
    private final FileContent fileContent;
    // For V3 tables: existing deletion vectors keyed by the data file they
    // reference. When a DELETE re-touches a data file that already has a DV,
    // the worker seeds the new DV's bitmap with the prior DV's positions and
    // the commit replaces the old DV, preserving Iceberg's one-DV-per-data-file
    // invariant. Empty for V2 tables and for first-time mutations.
    private final Map<String, DeleteFile> existingDeletionVectors;

    @JsonCreator
    public IcebergDeleteTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") IcebergTableName tableName,
            @JsonProperty("schema") PrestoIcebergSchema schema,
            @JsonProperty("partitionSpec") PrestoIcebergPartitionSpec partitionSpec,
            @JsonProperty("inputColumns") List<IcebergColumnHandle> inputColumns,
            @JsonProperty("outputPath") String outputPath,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("compressionCodec") HiveCompressionCodec compressionCodec,
            @JsonProperty("storageProperties") Map<String, String> storageProperties,
            @JsonProperty("sortOrder") List<SortField> sortOrder,
            @JsonProperty("materializedViewName") Optional<SchemaTableName> materializedViewName,
            @JsonProperty("fileContent") FileContent fileContent,
            @JsonProperty("existingDeletionVectors") Map<String, DeleteFile> existingDeletionVectors)
    {
        super(
                schemaName,
                tableName,
                schema,
                partitionSpec,
                inputColumns,
                outputPath,
                fileFormat,
                compressionCodec,
                storageProperties,
                sortOrder,
                materializedViewName);
        this.fileContent = requireNonNull(fileContent, "fileContent is null");
        this.existingDeletionVectors = existingDeletionVectors == null ? ImmutableMap.of() : ImmutableMap.copyOf(existingDeletionVectors);
    }

    @JsonProperty
    public FileContent getFileContent()
    {
        return fileContent;
    }

    @JsonProperty
    public Map<String, DeleteFile> getExistingDeletionVectors()
    {
        return existingDeletionVectors;
    }
}
