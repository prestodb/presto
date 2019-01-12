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
package com.facebook.presto.raptor.systemtables;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.TableStatsRow;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.raptor.systemtables.TableMetadataSystemTable.getColumnIndex;
import static com.facebook.presto.raptor.systemtables.TableMetadataSystemTable.getStringValue;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;

public class TableStatsSystemTable
        implements SystemTable
{
    private static final String SCHEMA_NAME = "table_schema";
    private static final String TABLE_NAME = "table_name";

    private static final ConnectorTableMetadata METADATA = new ConnectorTableMetadata(
            new SchemaTableName("system", "table_stats"),
            ImmutableList.<ColumnMetadata>builder()
                    .add(new ColumnMetadata(SCHEMA_NAME, createUnboundedVarcharType()))
                    .add(new ColumnMetadata(TABLE_NAME, createUnboundedVarcharType()))
                    .add(new ColumnMetadata("create_time", TIMESTAMP))
                    .add(new ColumnMetadata("update_time", TIMESTAMP))
                    .add(new ColumnMetadata("table_version", BIGINT))
                    .add(new ColumnMetadata("shard_count", BIGINT))
                    .add(new ColumnMetadata("row_count", BIGINT))
                    .add(new ColumnMetadata("compressed_size", BIGINT))
                    .add(new ColumnMetadata("uncompressed_size", BIGINT))
                    .build());

    private final MetadataDao dao;

    @Inject
    public TableStatsSystemTable(@ForMetadata IDBI dbi)
    {
        this.dao = onDemandDao(dbi, MetadataDao.class);
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(dao, constraint));
    }

    private static List<Page> buildPages(MetadataDao dao, TupleDomain<Integer> tupleDomain)
    {
        Map<Integer, NullableValue> domainValues = extractFixedValues(tupleDomain).orElse(ImmutableMap.of());
        String schemaName = getStringValue(domainValues.get(getColumnIndex(METADATA, SCHEMA_NAME)));
        String tableName = getStringValue(domainValues.get(getColumnIndex(METADATA, TABLE_NAME)));

        PageListBuilder pageBuilder = new PageListBuilder(METADATA.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toList()));

        for (TableStatsRow row : dao.getTableStatsRows(schemaName, tableName)) {
            pageBuilder.beginRow();
            VARCHAR.writeSlice(pageBuilder.nextBlockBuilder(), utf8Slice(row.getSchemaName()));
            VARCHAR.writeSlice(pageBuilder.nextBlockBuilder(), utf8Slice(row.getTableName()));
            TIMESTAMP.writeLong(pageBuilder.nextBlockBuilder(), row.getCreateTime());
            TIMESTAMP.writeLong(pageBuilder.nextBlockBuilder(), row.getUpdateTime());
            BIGINT.writeLong(pageBuilder.nextBlockBuilder(), row.getTableVersion());
            BIGINT.writeLong(pageBuilder.nextBlockBuilder(), row.getShardCount());
            BIGINT.writeLong(pageBuilder.nextBlockBuilder(), row.getRowCount());
            BIGINT.writeLong(pageBuilder.nextBlockBuilder(), row.getCompressedSize());
            BIGINT.writeLong(pageBuilder.nextBlockBuilder(), row.getUncompressedSize());
        }

        return pageBuilder.build();
    }
}
