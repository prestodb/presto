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
package com.facebook.presto.iceberg.vectors;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.HdfsFileIO;
import com.facebook.presto.iceberg.HdfsOutputFile;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.iceberg.RuntimeStatsMetricsReporter;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Utility class for building vector indexes from Iceberg table data.
 */
public class IcebergVectorIndexBuilder
{
    private static final Logger log = Logger.get(IcebergVectorIndexBuilder.class);
    private static final String VECTOR_INDEX_DIR = ".vector_index";
    private static final String ROW_ID_COLUMN = "row_id";

    private IcebergVectorIndexBuilder() {}

    /**
     * Internal class to hold vector data along with corresponding row IDs.
     */
    private static class VectorData
    {
        final List<float[]> vectors;
        final List<Long> rowIds;

        VectorData(List<float[]> vectors, List<Long> rowIds)
        {
            if (vectors.size() != rowIds.size()) {
                throw new IllegalStateException(
                        String.format("Vectors and row IDs must have the same size. Vectors: %d, Row IDs: %d",
                                vectors.size(), rowIds.size()));
            }
            this.vectors = vectors;
            this.rowIds = rowIds;
        }
    }

    /**
     * Builds a vector index from an Iceberg table column and saves it to S3.
     * The index is saved to the table's data location using Iceberg's FileIO,
     * which automatically handles S3 configuration.
     * Path format: [table_data_location]/.vector_index/vector_index.hnsw
     *
     * @param metadata The connector metadata
     * @param pageSourceProvider The page source provider
     * @param transactionHandle The transaction handle
     * @param session The connector session
     * @param schemaTableName The schema and table name
     * @param columnName The name of the column containing vector data
     * @param indexName The name of the index
     * @param catalogName The catalog name
     * @param similarityFunction The similarity function to use for the index
     * @param m The maximum number of connections per node in the graph
     * @param efConstruction The size of the dynamic candidate list during construction
     * @return The path to the saved index file
     */
    public static Path buildAndSaveVectorIndex(
            ConnectorMetadata metadata,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String columnName,
            String indexName,
            String catalogName,
            String similarityFunction,
            int m,
            int efConstruction) throws Exception
    {
        // 1. Get the Iceberg table
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, session, schemaTableName);
        // Get the table's HdfsFileIO for S3 operations
        HdfsFileIO hdfsFileIO = (HdfsFileIO) icebergTable.io();
        // Get the table's location
        String tableLocation;
        if (icebergTable instanceof BaseTable) {
            // Get location from metadata without filesystem access
            tableLocation = ((BaseTable) icebergTable).operations().current().location();
        }
        else {
            // Fallback to direct call
            tableLocation = icebergTable.location();
        }
        // Compute the S3 path for the index
        String indexDirPath = tableLocation + "/" + VECTOR_INDEX_DIR;
        String indexFileName = indexName + ".hnsw";
        String indexPath = indexDirPath + "/" + indexFileName;
        log.info("Vector index will be saved to S3 path: %s", indexPath);
        log.info("Table location: %s", tableLocation);
        // 2. Read vectors and row IDs from the table
        VectorData vectorData = readVectorsFromTable(
                metadata,
                pageSourceProvider,
                transactionHandle,
                session,
                schemaTableName,
                columnName);
        if (vectorData.vectors.isEmpty()) {
            throw new IllegalStateException("No vectors found in column: " + columnName);
        }
        log.info("Read %d vectors with corresponding row IDs", vectorData.vectors.size());
        // Normalize all vectors using L2 normalization
        log.info("Normalizing vectors using L2 normalization");
        for (float[] vector : vectorData.vectors) {
            CustomVectorFloat customVector = new CustomVectorFloat(vector);
            VectorUtil.l2normalize(customVector.toArrayVectorFloat());
        }
        // 3. Create vector values wrapper
        int dimension = vectorData.vectors.get(0).length;
        RandomAccessVectorValues ravv = new ListRandomAccessVectorValues(vectorData.vectors, dimension);
        // 4. Create similarity function
        VectorSimilarityFunction simFunction = getVectorSimilarityFunction(similarityFunction);
        // 5. Build the index
        log.info("Building vector index with %d vectors of dimension %d", vectorData.vectors.size(), dimension);
        // Create a BuildScoreProvider from the RandomAccessVectorValues and similarity function
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, simFunction);
        GraphIndexBuilder builder = new GraphIndexBuilder(bsp, ravv.dimension(), m, efConstruction, 4 * m, 1.2f, false, true);
        ImmutableGraphIndex index = builder.build(ravv);
        log.info("Vector index built successfully with %d nodes", index.size());
        // 6. Create node-to-row ID mapping
        NodeRowIdMapping mapping = new NodeRowIdMapping(vectorData.rowIds);
        log.info("Created node-to-row ID mapping with %d entries", mapping.size());
        // 7. Prepare mapping file path
        String mappingFileName = indexName + "_mapping.bin";
        String mappingPath = indexDirPath + "/" + mappingFileName;
        try {
            hdfsFileIO.newInputFile(indexPath).newStream().close();
            log.info("Existing index file found at %s - will be replaced", indexPath);
        }
        catch (Exception e) {
            log.info("No existing index file found at %s - creating new", indexPath);
        }
        try {
            hdfsFileIO.newInputFile(mappingPath).newStream().close();
            log.info("Existing mapping file found at %s - will be replaced", mappingPath);
        }
        catch (Exception e) {
            log.info("No existing mapping file found at %s - creating new", mappingPath);
        }
        // 8. Save index and mapping to S3 using Iceberg's FileIO with retry logic
        int maxRetries = 3;
        int retryDelayMs = 1000;
        Path localTempIndexPath = null;
        Path localTempMappingPath = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Saving index and mapping to S3 (attempt %d of %d)", attempt, maxRetries);
                // Create local temporary files
                localTempIndexPath = Files.createTempFile("vector-index-", ".tmp");
                localTempMappingPath = Files.createTempFile("vector-mapping-", ".tmp");
                try {
                    // Write the index to local temporary file
                    log.info("Writing index to local temporary file: %s", localTempIndexPath);
                    OnDiskGraphIndex.write(index, ravv, localTempIndexPath);

                    // Write the mapping to local temporary file
                    log.info("Writing mapping to local temporary file: %s", localTempMappingPath);
                    try (OutputStream mappingOut = Files.newOutputStream(localTempMappingPath)) {
                        mapping.save(mappingOut);
                    }

                    // Upload index to S3
                    log.info("Uploading index to S3: %s", indexPath);
                    HdfsOutputFile indexOutputFile = (HdfsOutputFile) hdfsFileIO.newOutputFile(indexPath);
                    try (OutputStream out = indexOutputFile.createOrOverwrite()) {
                        Files.copy(localTempIndexPath, out);
                    }
                    log.info("Vector index saved successfully to S3: %s", indexPath);
                    // Upload mapping to S3
                    log.info("Uploading mapping to S3: %s", mappingPath);
                    HdfsOutputFile mappingOutputFile = (HdfsOutputFile) hdfsFileIO.newOutputFile(mappingPath);
                    try (OutputStream out = mappingOutputFile.createOrOverwrite()) {
                        Files.copy(localTempMappingPath, out);
                    }
                    log.info("Node-to-row ID mapping saved successfully to S3: %s", mappingPath);
                    return Paths.get(indexPath);
                }
                finally {
                    // Clean up local temporary files
                    if (localTempIndexPath != null) {
                        try {
                            Files.delete(localTempIndexPath);
                        }
                        catch (IOException e) {
                            log.warn(e, "Failed to delete local temporary index file: %s", localTempIndexPath);
                        }
                    }
                    if (localTempMappingPath != null) {
                        try {
                            Files.delete(localTempMappingPath);
                        }
                        catch (IOException e) {
                            log.warn(e, "Failed to delete local temporary mapping file: %s", localTempMappingPath);
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error(e, "Error saving index and mapping to S3 (attempt %d of %d): %s",
                        attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs * attempt);
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
                else {
                    throw new RuntimeException("Failed to save vector index and mapping to S3 after " + maxRetries + " attempts", e);
                }
            }
        }
        throw new RuntimeException("Failed to save vector index and mapping to S3 after " + maxRetries + " attempts");
    }
    private static VectorSimilarityFunction getVectorSimilarityFunction(String similarityFunction)
    {
        switch (similarityFunction.toUpperCase()) {
            case "COSINE":
                return VectorSimilarityFunction.COSINE;
            case "DOT_PRODUCT":
                return VectorSimilarityFunction.DOT_PRODUCT;
            case "EUCLIDEAN":
                return VectorSimilarityFunction.EUCLIDEAN;
            default:
                throw new IllegalArgumentException("Unsupported similarity function: " + similarityFunction);
        }
    }
    /**
     * Reads vector data and row IDs from an Iceberg table.
     *
     * @return VectorData containing both vectors and their corresponding row IDs
     */
    private static VectorData readVectorsFromTable(
            ConnectorMetadata metadata,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String columnName) throws IOException
    {
        // Get the table handle
        IcebergTableHandle tableHandle = (IcebergTableHandle) metadata.getTableHandle(session, schemaTableName);
        if (tableHandle == null) {
            throw new IllegalArgumentException("Table not found: " + schemaTableName);
        }
        // Get TypeManager from metadata
        TypeManager typeManager = null;
        if (metadata instanceof IcebergAbstractMetadata) {
            typeManager = ((IcebergAbstractMetadata) metadata).getTypeManager();
        }
        if (typeManager == null) {
            throw new IllegalStateException("Could not get TypeManager from metadata");
        }
        // Get the Iceberg table
        Table icebergTable = IcebergUtil.getIcebergTable(metadata, session, schemaTableName);
        // Get all columns
        List<IcebergColumnHandle> columns = IcebergUtil.getColumns(
                icebergTable.schema(),
                icebergTable.spec(),
                typeManager);
        // Find the target vector column
        IcebergColumnHandle targetColumn = null;
        for (IcebergColumnHandle column : columns) {
            if (column.getName().equals(columnName)) {
                targetColumn = column;
                break;
            }
        }
        if (targetColumn == null) {
            throw new IllegalArgumentException("Vector column not found: " + columnName);
        }
        // Verify vector column type
        Type columnType = targetColumn.getType();
        if (!(columnType instanceof ArrayType)) {
            throw new IllegalArgumentException("Vector column must be an array type: " + columnName);
        }
        // Find the row_id column
        IcebergColumnHandle rowIdColumn = null;
        for (IcebergColumnHandle column : columns) {
            if (column.getName().equals(ROW_ID_COLUMN)) {
                rowIdColumn = column;
                break;
            }
        }
        if (rowIdColumn == null) {
            throw new IllegalArgumentException(
                    String.format("Required column '%s' not found in table %s. " +
                            "All tables using vector indexing must have a '%s' column of type BIGINT.",
                            ROW_ID_COLUMN, schemaTableName, ROW_ID_COLUMN));
        }
        // Verify row_id column type
        if (!rowIdColumn.getType().equals(com.facebook.presto.common.type.BigintType.BIGINT)) {
            throw new IllegalArgumentException(
                    String.format("Column '%s' must be of type BIGINT, but found: %s",
                            ROW_ID_COLUMN, rowIdColumn.getType()));
        }
        log.info("Reading vectors from column '%s' and row IDs from column '%s'", columnName, ROW_ID_COLUMN);
        // Create a table layout handle
        IcebergTableLayoutHandle layoutHandle = createTableLayoutHandle(tableHandle, columns);
        // Use TableScan API to get actual data files
        List<float[]> vectors = new ArrayList<>();
        List<Long> rowIds = new ArrayList<>();
        // Create a table scan to get the data files
        TableScan tableScan = icebergTable.newScan()
                .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()));
        // If the table has a current snapshot, use it
        if (icebergTable.currentSnapshot() != null) {
            tableScan = tableScan.useSnapshot(icebergTable.currentSnapshot().snapshotId());
        }
        // Get the data files
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                // Create a split for each data file
                IcebergSplit split = new IcebergSplit(
                        fileScanTask.file().path().toString(), // Use the actual file path
                        fileScanTask.start(),
                        fileScanTask.length(),
                        IcebergUtil.getFileFormat(icebergTable),
                        ImmutableList.of(),
                        IcebergUtil.getPartitionKeys(fileScanTask),
                        PartitionSpecParser.toJson(fileScanTask.spec()),
                        IcebergUtil.partitionDataFromStructLike(fileScanTask.spec(), fileScanTask.file().partition()).map(PartitionData::toJson),
                        NodeSelectionStrategy.NO_PREFERENCE,
                        SplitWeight.standard(),
                        fileScanTask.deletes().stream().map(DeleteFile::fromIceberg).collect(toImmutableList()),
                        Optional.empty(),
                        IcebergUtil.getDataSequenceNumber(fileScanTask.file()),
                        1,
                        false,
                        ImmutableList.of(),
                        0); // Use 1 to avoid division by zero
                // Read the data from this split - read both vector and row_id columns
                try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                        transactionHandle,
                        session,
                        split,
                        layoutHandle,
                        ImmutableList.of(targetColumn, rowIdColumn),
                        new SplitContext(false),
                        new RuntimeStats())) {
                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (page == null) {
                            continue;
                        }
                        // Block 0 is the vector column, Block 1 is the row_id column
                        Block vectorBlock = page.getBlock(0);
                        Block rowIdBlock = page.getBlock(1);
                        for (int position = 0; position < vectorBlock.getPositionCount(); position++) {
                            // Skip if vector is null
                            if (vectorBlock.isNull(position)) {
                                continue;
                            }
                            // Skip if row_id is null
                            if (rowIdBlock.isNull(position)) {
                                log.warn("Skipping row at position %d: row_id is null", position);
                                continue;
                            }
                            Block arrayBlock = vectorBlock.getBlock(position);
                            if (arrayBlock.getPositionCount() == 0) {
                                continue;
                            }
                            // Read the vector
                            float[] vector = new float[arrayBlock.getPositionCount()];
                            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                                if (arrayBlock.isNull(i)) {
                                    vector[i] = 0.0f;
                                }
                                else {
                                    vector[i] = ((Number) ((ArrayType) columnType).getElementType().getObjectValue(
                                            session.getSqlFunctionProperties(), arrayBlock, i)).floatValue();
                                }
                            }
                            // Read the row_id
                            long rowId = com.facebook.presto.common.type.BigintType.BIGINT.getLong(rowIdBlock, position);
                            // Add both to their respective lists (maintaining order)
                            vectors.add(vector);
                            rowIds.add(rowId);
                        }
                    }
                }
            }
        }
        log.info("Read %d vectors with %d row IDs from table", vectors.size(), rowIds.size());
        return new VectorData(vectors, rowIds);
    }
    /**
     * Creates a minimal IcebergTableLayoutHandle for reading data.
     */
    private static IcebergTableLayoutHandle createTableLayoutHandle(IcebergTableHandle tableHandle, List<IcebergColumnHandle> columns)
    {
        return new IcebergTableLayoutHandle.Builder()
                .setPartitionColumns(ImmutableList.of())
                .setDataColumns(ImmutableList.of())
                .setDomainPredicate(TupleDomain.all())
                .setRemainingPredicate(new ConstantExpression(true, BooleanType.BOOLEAN))
                .setPredicateColumns(ImmutableMap.of())
                .setRequestedColumns(Optional.empty())
                .setPushdownFilterEnabled(false)
                .setPartitionColumnPredicate(TupleDomain.all())
                .setPartitions(Optional.empty())
                .setTable(tableHandle)
                .build();
    }
}
