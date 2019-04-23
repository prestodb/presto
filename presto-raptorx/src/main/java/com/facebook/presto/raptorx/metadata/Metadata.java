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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.util.CloseableIterator;
import com.facebook.presto.raptorx.util.ColumnRange;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Metadata
{
    long getCurrentCommitId();

    long nextTransactionId();

    long nextSchemaId();

    long nextTableId();

    long nextViewId();

    long nextColumnId();

    void registerTransaction(long transactionId);

    void registerTransactionTable(long transactionId, long tableId);

    long createDistribution(Optional<String> distributionName, List<Type> columnTypes, List<Long> bucketNodes);

    DistributionInfo getDistributionInfo(long distributionId);

    Optional<DistributionInfo> getDistributionInfo(String distributionName);

    Optional<Long> getSchemaId(long commitId, String schemaName);

    Optional<Long> getTableId(long commitId, long schemaId, String tableName);

    Optional<Long> getViewId(long commitId, long schemaId, String viewName);

    Collection<String> listSchemas(long commitId);

    Collection<String> listTables(long commitId, long schemaId);

    Collection<String> listViews(long commitId, long schemaId);

    SchemaInfo getSchemaInfo(long commitId, long schemaId);

    TableInfo getTableInfo(long commitId, long tableId);

    TableInfo getTableInfoForBgJob(long tableId);

    ViewInfo getViewInfo(long commitId, long viewId);

    Collection<TableStats> listTableStats(long commitId, Optional<Long> schemaId, Optional<Long> tableId);

    long getChunkRowCount(long commitId, long tableId, Set<Long> chunkIds);

    Collection<ChunkMetadata> getChunks(
            long commitId,
            long tableId,
            Collection<ChunkMetadata> addedChunks,
            Set<Long> deletedChunks);

    CloseableIterator<BucketChunks> getBucketChunks(
            long commitId,
            long tableId,
            Collection<ChunkInfo> addedChunks,
            Set<Long> deletedChunks,
            TupleDomain<Long> constraint,
            boolean merged);

    List<DistributionInfo> getActiveDistributions();

    long getDistributionSizeInBytes(long distributionId);

    List<BucketNode> getBucketNodes(long distibutionId);

    void updateBucketAssignment(long distributionId, int bucketNumber, long nodeId);

    List<ColumnRange> getColumnRanges(long commitId, long tableId, List<ColumnInfo> columns);
}
