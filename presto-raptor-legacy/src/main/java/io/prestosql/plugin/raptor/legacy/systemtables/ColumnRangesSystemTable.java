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

import com.facebook.presto.raptor.RaptorTableHandle;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.VerifyException;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.DBIException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.maxColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.minColumn;
import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ColumnRangesSystemTable
        implements SystemTable
{
    private static final String MIN_COLUMN_SUFFIX = "_min";
    private static final String MAX_COLUMN_SUFFIX = "_max";
    private static final String COLUMN_RANGES_TABLE_SUFFIX = "$column_ranges";

    private final IDBI dbi;
    private final RaptorTableHandle sourceTable;
    private final List<TableColumn> indexedRaptorColumns;
    private final ConnectorTableMetadata tableMetadata;

    public ColumnRangesSystemTable(RaptorTableHandle sourceTable, IDBI dbi)
    {
        this.sourceTable = requireNonNull(sourceTable, "sourceTable is null");
        this.dbi = requireNonNull(dbi, "dbi is null");

        this.indexedRaptorColumns = dbi.onDemand(MetadataDao.class)
                .listTableColumns(sourceTable.getTableId()).stream()
                .filter(column -> isIndexedType(column.getDataType()))
                .collect(toImmutableList());
        List<ColumnMetadata> systemTableColumns = indexedRaptorColumns.stream()
                .flatMap(column -> Stream.of(
                        new ColumnMetadata(column.getColumnName() + MIN_COLUMN_SUFFIX, column.getDataType(), null, false),
                        new ColumnMetadata(column.getColumnName() + MAX_COLUMN_SUFFIX, column.getDataType(), null, false)))
                .collect(toImmutableList());
        SchemaTableName tableName = new SchemaTableName(sourceTable.getSchemaName(), sourceTable.getTableName() + COLUMN_RANGES_TABLE_SUFFIX);
        this.tableMetadata = new ConnectorTableMetadata(tableName, systemTableColumns);
    }

    public static Optional<SchemaTableName> getSourceTable(SchemaTableName tableName)
    {
        if (tableName.getTableName().endsWith(COLUMN_RANGES_TABLE_SUFFIX) &&
                !tableName.getTableName().equals(COLUMN_RANGES_TABLE_SUFFIX)) {
            int tableNameLength = tableName.getTableName().length() - COLUMN_RANGES_TABLE_SUFFIX.length();
            return Optional.of(new SchemaTableName(
                    tableName.getSchemaName(),
                    tableName.getTableName().substring(0, tableNameLength)));
        }
        return Optional.empty();
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        String metadataSqlQuery = getColumnRangesMetadataSqlQuery(sourceTable, indexedRaptorColumns);
        List<Type> columnTypes = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        PageListBuilder pageListBuilder = new PageListBuilder(columnTypes);

        try (Connection connection = dbi.open().getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(metadataSqlQuery)) {
            if (resultSet.next()) {
                pageListBuilder.beginRow();
                for (int i = 0; i < columnTypes.size(); ++i) {
                    BlockBuilder blockBuilder = pageListBuilder.nextBlockBuilder();
                    Type columnType = columnTypes.get(i);
                    if (columnType.equals(BIGINT) || columnType.equals(DATE) || columnType.equals(TIMESTAMP)) {
                        long value = resultSet.getLong(i + 1);
                        if (!resultSet.wasNull()) {
                            columnType.writeLong(blockBuilder, value);
                        }
                        else {
                            blockBuilder.appendNull();
                        }
                    }
                    else if (columnType.equals(BOOLEAN)) {
                        boolean value = resultSet.getBoolean(i + 1);
                        if (!resultSet.wasNull()) {
                            BOOLEAN.writeBoolean(blockBuilder, value);
                        }
                        else {
                            blockBuilder.appendNull();
                        }
                    }
                    else {
                        throw new VerifyException("Unknown or unsupported column type: " + columnType);
                    }
                }
            }
        }
        catch (SQLException | DBIException e) {
            throw metadataError(e);
        }

        return new FixedPageSource(pageListBuilder.build());
    }

    private static boolean isIndexedType(Type type)
    {
        // We only consider the following types in the column_ranges system table
        // Exclude INTEGER because we don't collect column stats for INTEGER type.
        // Exclude DOUBLE because Java double is not completely compatible with MySQL double
        // Exclude VARCHAR because they can be truncated
        return type.equals(BOOLEAN) || type.equals(BIGINT) || type.equals(DATE) || type.equals(TIMESTAMP);
    }

    private static String getColumnRangesMetadataSqlQuery(RaptorTableHandle raptorTableHandle, List<TableColumn> raptorColumns)
    {
        String columns = raptorColumns.stream()
                .flatMap(column -> Stream.of(
                        format("min(%s)", minColumn(column.getColumnId())),
                        format("max(%s)", maxColumn(column.getColumnId()))))
                .collect(joining(", "));
        return format("SELECT %s FROM %s", columns, shardIndexTable(raptorTableHandle.getTableId()));
    }
}
