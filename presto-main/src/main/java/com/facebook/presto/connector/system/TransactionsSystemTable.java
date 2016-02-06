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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TransactionsSystemTable
        implements SystemTable
{
    public static final SchemaTableName TRANSACTIONS_TABLE_NAME = new SchemaTableName("runtime", "transactions");

    private final ConnectorTableMetadata transactionsTable;
    private final TransactionManager transactionManager;

    @Inject
    public TransactionsSystemTable(TypeManager typeManager, TransactionManager transactionManager)
    {
        this.transactionsTable = tableMetadataBuilder(TRANSACTIONS_TABLE_NAME)
                .column("transaction_id", VARCHAR)
                .column("isolation_level", VARCHAR)
                .column("read_only", BOOLEAN)
                .column("auto_commit_context", BOOLEAN)
                .column("create_time", TIMESTAMP)
                .column("idle_time_secs", BIGINT)
                .column("written_catalog", VARCHAR)
                .column("catalogs", typeManager.getParameterizedType(ARRAY, ImmutableList.of(VARCHAR.getTypeSignature()), ImmutableList.of()))
                .build();
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return transactionsTable;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(transactionsTable);
        for (TransactionInfo info : transactionManager.getAllTransactionInfos()) {
            table.addRow(
                    info.getTransactionId().toString(),
                    info.getIsolationLevel().toString(),
                    info.isReadOnly(),
                    info.isAutoCommitContext(),
                    info.getCreateTime().getMillis(),
                    (long) info.getIdleTime().getValue(TimeUnit.SECONDS),
                    info.getWrittenConnectorId().orElse(null),
                    createStringsBlock(info.getConnectorIds()));
        }
        return table.build().cursor();
    }

    private static Block createStringsBlock(List<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), values.size());
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }
        return builder.build();
    }
}
