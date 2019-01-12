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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableList;
import io.prestosql.connector.ConnectorId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.InMemoryRecordSet.Builder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.transaction.TransactionInfo;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
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
                .column("transaction_id", createUnboundedVarcharType())
                .column("isolation_level", createUnboundedVarcharType())
                .column("read_only", BOOLEAN)
                .column("auto_commit_context", BOOLEAN)
                .column("create_time", TIMESTAMP)
                .column("idle_time_secs", BIGINT)
                .column("written_catalog", createUnboundedVarcharType())
                .column("catalogs", typeManager.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.of(createUnboundedVarcharType().getTypeSignature()))))
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
                    info.getWrittenConnectorId().map(ConnectorId::getCatalogName).orElse(null),
                    createStringsBlock(info.getConnectorIds()));
        }
        return table.build().cursor();
    }

    private static Block createStringsBlock(List<ConnectorId> values)
    {
        VarcharType varchar = createUnboundedVarcharType();
        BlockBuilder builder = varchar.createBlockBuilder(null, values.size());
        for (ConnectorId value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                varchar.writeString(builder, value.getCatalogName());
            }
        }
        return builder.build();
    }
}
