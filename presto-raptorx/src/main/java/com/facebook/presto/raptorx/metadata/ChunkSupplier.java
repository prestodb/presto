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
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.PeekingIterator;
import io.airlift.log.Logger;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.IndexWriter.chunkIndexTable;
import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.Closeables.closeWithSuppression;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.enableStreamingResults;
import static com.facebook.presto.raptorx.util.DatabaseUtil.metadataError;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class ChunkSupplier
{
    private static final Logger log = Logger.get(ChunkSupplier.class);

    private final List<Jdbi> shardDbi;

    @Inject
    public ChunkSupplier(Database database)
    {
        this.shardDbi = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .collect(toImmutableList());
    }

    public Collection<ChunkMetadata> getChunks(
            long commitId,
            long tableId,
            Collection<ChunkMetadata> addedChunks,
            Set<Long> deletedChunks)
    {
        Jdbi dbi = shardDbi.get(tableShard(tableId, shardDbi.size()));
        ChunkSupplierDao dao = dbi.onDemand(ChunkSupplierDao.class);

        ImmutableList.Builder<ChunkMetadata> list = ImmutableList.builder();
        list.addAll(addedChunks);

        for (ChunkMetadata chunk : dao.getChunks(commitId, tableId)) {
            if (!deletedChunks.contains(chunk.getChunkId())) {
                list.add(chunk);
            }
        }

        return list.build();
    }

    public CloseableIterator<BucketChunks> getBucketChunks(
            long commitId,
            long tableId,
            Collection<ChunkInfo> addedChunks,
            Set<Long> deletedChunks,
            TupleDomain<Long> constraint,
            boolean merged)
    {
        ChunkPredicate predicate = ChunkPredicate.create(constraint);
        String sql = format("" +
                        "SELECT bucket_number, chunk_id\n" +
                        "FROM %s\n" +
                        "WHERE start_commit_id <= ?\n" +
                        "  AND (end_commit_id > ? OR end_commit_id IS NULL)\n" +
                        "  AND %s%s",
                chunkIndexTable(tableId),
                predicate.getPredicate(),
                merged ? "\nORDER BY bucket_number" : "");

        Jdbi dbi = shardDbi.get(tableShard(tableId, shardDbi.size()));
        Connection connection = dbi.open().getConnection();
        PreparedStatement statement = null;
        ResultSet resultSet;

        try {
            statement = connection.prepareStatement(sql);
            statement.setLong(1, commitId);
            statement.setLong(2, commitId);
            predicate.bind(statement, 3);
            enableStreamingResults(statement);
            log.debug("Executing SQL: %s", sql.replaceAll("\\s+", " "));
            resultSet = statement.executeQuery();
        }
        catch (SQLException e) {
            closeWithSuppression(e, statement, connection);
            throw metadataError(e);
        }
        catch (Throwable t) {
            closeWithSuppression(t, statement, connection);
            throw t;
        }

        return merged
                ? new MergedChunkIterator(resultSet, addedChunks, deletedChunks)
                : new FlatChunkIterator(resultSet, addedChunks, deletedChunks);
    }

    private abstract static class AbstractChunkIterator
            extends AbstractIterator<BucketChunks>
            implements CloseableIterator<BucketChunks>
    {
        private final ResultSet resultSet;
        protected final PeekingIterator<BucketChunk> rows = new BucketChunkIterator();

        protected AbstractChunkIterator(ResultSet resultSet)
        {
            this.resultSet = requireNonNull(resultSet, "resultSet is null");
        }

        @SuppressWarnings({"UnusedDeclaration", "EmptyTryBlock"})
        @Override
        public final void close()
        {
            // use try-with-resources to close everything properly
            try (Connection connection = this.resultSet.getStatement().getConnection();
                    Statement statement = this.resultSet.getStatement();
                    ResultSet resultSet = this.resultSet) {
                // do nothing
            }
            catch (SQLException e) {
                log.warn(e, "Error closing connection");
            }
        }

        private class BucketChunkIterator
                extends AbstractIterator<BucketChunk>
                implements PeekingIterator<BucketChunk>
        {
            @Override
            protected BucketChunk computeNext()
            {
                try {
                    if (!resultSet.next()) {
                        return endOfData();
                    }
                    return new BucketChunk(
                            resultSet.getInt("bucket_number"),
                            resultSet.getLong("chunk_id"));
                }
                catch (SQLException e) {
                    throw metadataError(e);
                }
            }
        }
    }

    private static class FlatChunkIterator
            extends AbstractChunkIterator
    {
        private final Iterator<BucketChunks> addedChunks;
        private final Set<Long> deletedChunks;

        public FlatChunkIterator(ResultSet resultSet, Collection<ChunkInfo> addedChunks, Set<Long> deletedChunks)
        {
            super(resultSet);
            this.addedChunks = addedChunks.stream()
                    .map(chunk -> new BucketChunks(chunk.getBucketNumber(), chunk.getChunkId()))
                    .collect(toList())
                    .iterator();
            this.deletedChunks = ImmutableSet.copyOf(deletedChunks);
        }

        @Override
        protected BucketChunks computeNext()
        {
            if (addedChunks.hasNext()) {
                return addedChunks.next();
            }

            while (rows.hasNext()) {
                BucketChunk row = rows.next();
                if (!deletedChunks.contains(row.getChunkId())) {
                    return new BucketChunks(row.getBucketNumber(), row.getChunkId());
                }
            }

            return endOfData();
        }
    }

    private static class MergedChunkIterator
            extends AbstractChunkIterator
    {
        private final Map<Integer, Set<Long>> addedChunks;
        private final Set<Long> deletedChunks;

        public MergedChunkIterator(ResultSet resultSet, Collection<ChunkInfo> addedChunks, Set<Long> deletedChunks)
        {
            super(resultSet);
            this.addedChunks = addedChunks.stream().collect(groupingBy(
                    ChunkInfo::getBucketNumber,
                    HashMap::new,
                    mapping(ChunkInfo::getChunkId, toCollection(HashSet::new))));
            this.deletedChunks = ImmutableSet.copyOf(deletedChunks);
        }

        @Override
        protected BucketChunks computeNext()
        {
            ImmutableSet.Builder<Long> chunkIds = ImmutableSet.builder();
            int bucketNumber = -1;

            while (rows.hasNext()) {
                if (deletedChunks.contains(rows.peek().getChunkId())) {
                    rows.next();
                    continue;
                }

                if ((bucketNumber != -1) && (rows.peek().getBucketNumber() != bucketNumber)) {
                    break;
                }

                BucketChunk row = rows.next();
                bucketNumber = row.getBucketNumber();
                chunkIds.add(row.getChunkId());
            }

            if (bucketNumber != -1) {
                Set<Long> added = addedChunks.remove(bucketNumber);
                if (added != null) {
                    chunkIds.addAll(added);
                }
                return new BucketChunks(bucketNumber, chunkIds.build());
            }

            if (!addedChunks.isEmpty()) {
                Iterator<Entry<Integer, Set<Long>>> iterator = addedChunks.entrySet().iterator();
                Entry<Integer, Set<Long>> entry = iterator.next();
                iterator.remove();
                return new BucketChunks(entry.getKey(), entry.getValue());
            }

            return endOfData();
        }
    }

    private static class BucketChunk
    {
        private final int bucketNumber;
        private final long chunkId;

        private BucketChunk(int bucketNumber, long chunkId)
        {
            this.bucketNumber = bucketNumber;
            this.chunkId = chunkId;
        }

        public int getBucketNumber()
        {
            return bucketNumber;
        }

        public long getChunkId()
        {
            return chunkId;
        }
    }
}
