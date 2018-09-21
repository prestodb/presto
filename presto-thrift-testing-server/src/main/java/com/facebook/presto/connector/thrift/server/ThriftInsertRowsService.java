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
package com.facebook.presto.connector.thrift.server;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.PrestoThriftColumnMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPage;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.connector.thrift.server.ThriftTpchService.SPLIT_INFO_CODEC;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class ThriftInsertRowsService
        implements PrestoThriftService
{
    private static final List<String> SCHEMA = Collections.singletonList("matiash");
    private static final List<String> SCHEMA_TABLES = ImmutableList.of("test_table");
    private static final List<PrestoThriftPage> TEST_TABLE = new ArrayList<>();
    private static final Map<String, List<PrestoThriftPage>> INSERT_BUFFER = new HashMap<>();
    private static final List<String> COLUMN_NAMES = ImmutableList.of("tile_quadkey", "value", "x", "y", "zoom_level");
    private static final List<Type> COLUMN_TYPES = ImmutableList.of(VARCHAR, DOUBLE, INTEGER, INTEGER, INTEGER);

    @Override
    public List<String> listSchemaNames()
    {
        return SCHEMA;
    }

    @Override
    public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
    {
        if (SCHEMA.contains(schemaNameOrNull.getSchemaName())) {
            List<PrestoThriftSchemaTableName> tables = new ArrayList<PrestoThriftSchemaTableName>() {};
            for (String tableName : SCHEMA_TABLES) {
                tables.add(new PrestoThriftSchemaTableName(SCHEMA.get(0), tableName));
            }
            return tables;
        }
        return ImmutableList.of();
    }

    @Override
    public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
    {
        if (!SCHEMA.contains(schemaTableName.getSchemaName()) || !SCHEMA_TABLES.contains(schemaTableName.getTableName())) {
            return new PrestoThriftNullableTableMetadata(null);
        }

        List<PrestoThriftColumnMetadata> columnMetadata = new ArrayList<>();
        for (int i = 0; i < COLUMN_NAMES.size(); i++) {
            columnMetadata.add(new PrestoThriftColumnMetadata(COLUMN_NAMES.get(i), getColumnTypeAsString(COLUMN_TYPES.get(i)), null, false));
        }
        return new PrestoThriftNullableTableMetadata(new PrestoThriftTableMetadata(schemaTableName, columnMetadata, null, null, Collections.singletonList("tile_quadkey")));
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getSplits(
            PrestoThriftSchemaTableName schemaTableName,
            PrestoThriftNullableColumnSet desiredColumns,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
    {
        return immediateFuture(getSplitsSync(schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken));
    }

    private PrestoThriftSplitBatch getSplitsSync(
            PrestoThriftSchemaTableName schemaTableName,
            PrestoThriftNullableColumnSet desiredColumns,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
    {
        SplitInfo splitInfo = SplitInfo.normalSplit(schemaTableName.getSchemaName(), schemaTableName.getTableName(), 1, 1);
        List<PrestoThriftSplit> splits = Collections.singletonList(new PrestoThriftSplit(new PrestoThriftId(SPLIT_INFO_CODEC.toJsonBytes(splitInfo)), ImmutableList.of()));
        return new PrestoThriftSplitBatch(splits, null);
    }

    @Override
    public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(
            PrestoThriftSchemaTableName schemaTableName,
            List<String> indexColumnNames,
            List<String> outputColumnNames,
            PrestoThriftPageResult keys,
            PrestoThriftTupleDomain outputConstraint,
            int maxSplitCount,
            PrestoThriftNullableToken nextToken)
    {
        return immediateFuture(getIndexSplitsInternal());
    }

    @Override
    public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
    {
        return immediateFuture(getRowsSync(splitId, columns, maxBytes, nextToken));
    }

    private PrestoThriftPageResult getRowsSync(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
    {
        List<PrestoThriftBlock> blocks = new ArrayList<>();
        int rowCount = 0;
        for (PrestoThriftPage pageResult : TEST_TABLE) {
            blocks.addAll(pageResult.getColumnBlocks());
            rowCount += pageResult.getRowCount();
        }
        return new PrestoThriftPageResult(blocks, rowCount, null);
    }

    @Override
    public ListenableFuture<Void> addRows(PrestoThriftSchemaTableName schemaTableName, PrestoThriftPage page, String insertId)
    {
        if (SCHEMA.contains(schemaTableName.getSchemaName()) && SCHEMA_TABLES.contains(schemaTableName.getTableName())) {
            if (!INSERT_BUFFER.containsKey(insertId)) {
                INSERT_BUFFER.put(insertId, new ArrayList<>());
            }
            INSERT_BUFFER.get(insertId).add(page);
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> finishAddRows(String insertId)
    {
        if (INSERT_BUFFER.containsKey(insertId)) {
            TEST_TABLE.addAll(INSERT_BUFFER.get(insertId));
        }
        return immediateFuture(null);
    }

    @Override
    public ListenableFuture<Void> abortAddRows(String insertId)
    {
        INSERT_BUFFER.remove(insertId);
        return immediateFuture(null);
    }

    private String getColumnTypeAsString(Type columnType)
    {
        return columnType.getTypeSignature().toString();
    }

    private PrestoThriftSplitBatch getIndexSplitsInternal()
    {
        throw new UnsupportedOperationException("getIndexSplits is unsupported");
    }
}
