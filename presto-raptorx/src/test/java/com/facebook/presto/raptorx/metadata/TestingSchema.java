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

import com.facebook.presto.raptorx.storage.ChunkIdSequence;
import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.nCopies;

public final class TestingSchema
{
    private TestingSchema() {}

    public static void createTestingSchema(TestingEnvironment environment)
    {
        SequenceManager sequenceManager = environment.getSequenceManager();
        Metadata metadata = environment.getMetadata();
        TransactionWriter transactionWriter = environment.getTransactionWriter();
        NodeIdCache nodeIdCache = environment.getNodeIdCache();
        ChunkIdSequence chunkIdSequence = () -> sequenceManager.nextValue("chunk_id", 100);

        long nodeId = nodeIdCache.getNodeId("node");
        long distributionId = metadata.createDistribution(Optional.empty(), ImmutableList.of(), nCopies(5, nodeId));

        Transaction transaction = new Transaction(metadata, metadata.nextTransactionId(), metadata.getCurrentCommitId());

        transaction.createSchema("tpch");
        long schemaId = transaction.getSchemaId("tpch").orElseThrow(AssertionError::new);

        long tableId = metadata.nextTableId();
        transaction.createTable(
                tableId,
                schemaId,
                "orders",
                distributionId,
                OptionalLong.empty(),
                CompressionType.LZ4,
                System.currentTimeMillis(),
                Optional.empty(),
                ImmutableList.<ColumnInfo>builder()
                        .add(column(transaction, "orderkey", BIGINT, 1))
                        .add(column(transaction, "customerkey", BIGINT, 2))
                        .add(column(transaction, "orderstatus", createVarcharType(1), 3))
                        .add(column(transaction, "totalprice", DOUBLE, 4))
                        .add(column(transaction, "orderdate", DATE, 5))
                        .build());

        transaction.insertChunks(tableId, ImmutableList.<ChunkInfo>builder()
                .add(new ChunkInfo(chunkIdSequence.nextChunkId(), 0, ImmutableList.of(), 3, 52, 122, 0))
                .add(new ChunkInfo(chunkIdSequence.nextChunkId(), 1, ImmutableList.of(), 8, 151, 277, 0))
                .add(new ChunkInfo(chunkIdSequence.nextChunkId(), 2, ImmutableList.of(), 5, 127, 229, 0))
                .build());

        transaction.createView(schemaId, "v_orders", "test", System.currentTimeMillis(), Optional.empty());

        transactionWriter.write(transaction.getActions(), OptionalLong.empty());
    }

    private static ColumnInfo column(Transaction transaction, String name, Type type, int ordinal)
    {
        return new ColumnInfo(transaction.nextColumnId(), name, type, Optional.empty(), ordinal, OptionalInt.empty(), OptionalInt.empty());
    }
}
