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
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergInsertTableHandle
        extends IcebergWritableTableHandle
        implements ConnectorInsertTableHandle
{
    private final List<String> insertedColumns;

    // For V3 tables: existing deletion vectors keyed by the data file they
    // reference, captured at beginMerge from the same snapshot the MERGE reads.
    // (UPDATE reaches this path only via the coordinator's UPDATE-through-MERGE
    // rewrite; beginUpdate itself returns an IcebergOutputTableHandle and does
    // not seed this map.) When a mutation re-touches a data file that already
    // has a DV, the worker seeds the new DV with the prior DV's positions and
    // the commit replaces the old DV, preserving Iceberg's one-DV-per-data-file
    // invariant. Empty for plain INSERT/CREATE and for first-time mutations.
    private final Map<String, DeleteFile> existingDeletionVectors;

    public IcebergInsertTableHandle(
            String schemaName,
            IcebergTableName tableName,
            PrestoIcebergSchema schema,
            PrestoIcebergPartitionSpec partitionSpec,
            List<IcebergColumnHandle> inputColumns,
            String outputPath,
            FileFormat fileFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> storageProperties,
            List<SortField> sortOrder,
            Optional<SchemaTableName> materializedViewName)
    {
        this(schemaName, tableName, schema, partitionSpec, inputColumns, outputPath,
                fileFormat, compressionCodec, storageProperties, sortOrder, materializedViewName, false, List.of(), ImmutableMap.of());
    }

    public IcebergInsertTableHandle(
            String schemaName,
            IcebergTableName tableName,
            PrestoIcebergSchema schema,
            PrestoIcebergPartitionSpec partitionSpec,
            List<IcebergColumnHandle> inputColumns,
            String outputPath,
            FileFormat fileFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> storageProperties,
            List<SortField> sortOrder,
            Optional<SchemaTableName> materializedViewName,
            List<String> insertedColumns)
    {
        this(schemaName, tableName, schema, partitionSpec, inputColumns, outputPath,
                fileFormat, compressionCodec, storageProperties, sortOrder, materializedViewName, false, insertedColumns, ImmutableMap.of());
    }

    public IcebergInsertTableHandle(
            String schemaName,
            IcebergTableName tableName,
            PrestoIcebergSchema schema,
            PrestoIcebergPartitionSpec partitionSpec,
            List<IcebergColumnHandle> inputColumns,
            String outputPath,
            FileFormat fileFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> storageProperties,
            List<SortField> sortOrder,
            Optional<SchemaTableName> materializedViewName,
            boolean fullRefreshRequired)
    {
        this(schemaName, tableName, schema, partitionSpec, inputColumns, outputPath,
                fileFormat, compressionCodec, storageProperties, sortOrder, materializedViewName, fullRefreshRequired, List.of(), ImmutableMap.of());
    }

    // Factory for the V3 merge path (beginMerge): supplies the existing DV map for
    // a merge-on-read MERGE (and UPDATE via the coordinator's UPDATE-through-MERGE
    // rewrite). A named factory (rather than yet another 12-arg constructor
    // overload distinguished only by the last parameter's type) keeps call sites
    // unambiguous. Delegates with false fullRefreshRequired and default
    // (input-derived) insertedColumns.
    public static IcebergInsertTableHandle forMergeOnRead(
            String schemaName,
            IcebergTableName tableName,
            PrestoIcebergSchema schema,
            PrestoIcebergPartitionSpec partitionSpec,
            List<IcebergColumnHandle> inputColumns,
            String outputPath,
            FileFormat fileFormat,
            HiveCompressionCodec compressionCodec,
            Map<String, String> storageProperties,
            List<SortField> sortOrder,
            Optional<SchemaTableName> materializedViewName,
            Map<String, DeleteFile> existingDeletionVectors)
    {
        return new IcebergInsertTableHandle(schemaName, tableName, schema, partitionSpec, inputColumns, outputPath,
                fileFormat, compressionCodec, storageProperties, sortOrder, materializedViewName, false, List.of(), existingDeletionVectors);
    }

    @JsonCreator
    public IcebergInsertTableHandle(
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
            @JsonProperty("fullRefreshRequired") boolean fullRefreshRequired,
            @JsonProperty("insertedColumns") List<String> insertedColumns,
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
                materializedViewName,
                fullRefreshRequired);
        this.insertedColumns = ImmutableList.copyOf(requireNonNull(insertedColumns, "insertedColumns is null"));
        // Tolerate a missing key in payloads produced by older binaries.
        this.existingDeletionVectors = existingDeletionVectors == null
                ? ImmutableMap.of()
                : ImmutableMap.copyOf(existingDeletionVectors);
    }

    @JsonProperty
    public List<String> getInsertedColumns()
    {
        return insertedColumns;
    }

    @JsonProperty
    public Map<String, DeleteFile> getExistingDeletionVectors()
    {
        return existingDeletionVectors;
    }
}
