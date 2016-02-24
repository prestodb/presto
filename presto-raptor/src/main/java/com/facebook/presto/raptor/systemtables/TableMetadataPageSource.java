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

import com.facebook.presto.raptor.metadata.ColumnMetadataRow;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.TableMetadataRow;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;
import io.airlift.slice.Slice;
import org.skife.jdbi.v2.IDBI;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_CORRUPT_METADATA;
import static com.facebook.presto.raptor.util.DatabaseUtil.onDemandDao;
import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.peekingIterator;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TableMetadataPageSource
        implements ConnectorPageSource
{
    public static final String SCHEMA_NAME = "table_schema";
    public static final String TABLE_NAME = "table_name";

    private final List<Type> types;
    private final TupleDomain<Integer> tupleDomain;
    private final ConnectorTableMetadata tableMetadata;
    private final MetadataDao dao;

    private boolean closed;

    private Iterator<Page> pageIterator;

    public TableMetadataPageSource(IDBI dbi, TupleDomain<Integer> tupleDomain, ConnectorTableMetadata tableMetadata)
    {
        this.dao = onDemandDao(requireNonNull(dbi, "dbi is null"), MetadataDao.class);
        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
        this.types = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType)
                .collect(toList());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        assureLoaded();
        return !pageIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        assureLoaded();
        return pageIterator.next();
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void assureLoaded()
    {
        checkState(!closed, "TableMetadataPageSource is closed");
        if (pageIterator != null) {
            return;
        }
        pageIterator = loadPages(tupleDomain);
    }

    private Iterator<Page> loadPages(TupleDomain<Integer> tupleDomain)
    {
        Map<Integer, Domain> domains = tupleDomain.getDomains().get();
        Domain schemaNameDomain = domains.get(getColumnIndex(tableMetadata, SCHEMA_NAME));
        Domain tableNameDomain = domains.get(getColumnIndex(tableMetadata, TABLE_NAME));

        String schemaName = schemaNameDomain == null ? null : getStringValue(schemaNameDomain.getSingleValue()).toLowerCase(ENGLISH);
        String tableName = tableNameDomain == null ? null : getStringValue(tableNameDomain.getSingleValue()).toLowerCase(ENGLISH);

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(types);

        List<TableMetadataRow> tableRows = dao.getTableMetadataRows(schemaName, tableName);
        PeekingIterator<ColumnMetadataRow> columnRowIterator = peekingIterator(dao.getColumnMetadataRows(schemaName, tableName).iterator());

        for (TableMetadataRow tableRow : tableRows) {
            while (columnRowIterator.hasNext() && columnRowIterator.peek().getTableId() < tableRow.getTableId()) {
                columnRowIterator.next();
            }

            String temporalColumnName = null;
            SortedMap<Integer, String> sortColumnNames = new TreeMap<>();
            SortedMap<Integer, String> bucketColumnNames = new TreeMap<>();
            OptionalLong temporalColumnId = tableRow.getTemporalColumnId();
            while (columnRowIterator.hasNext() && columnRowIterator.peek().getTableId() == tableRow.getTableId()) {
                ColumnMetadataRow columnRow = columnRowIterator.next();
                if (temporalColumnId.isPresent() && columnRow.getColumnId() == temporalColumnId.getAsLong()) {
                    temporalColumnName = columnRow.getColumnName();
                }
                OptionalInt sortOrdinalPosition = columnRow.getSortOrdinalPosition();
                if (sortOrdinalPosition.isPresent()) {
                    sortColumnNames.put(sortOrdinalPosition.getAsInt(), columnRow.getColumnName());
                }
                OptionalInt bucketOrdinalPosition = columnRow.getBucketOrdinalPosition();
                if (bucketOrdinalPosition.isPresent()) {
                    bucketColumnNames.put(bucketOrdinalPosition.getAsInt(), columnRow.getColumnName());
                }
            }

            pageBuilder.declarePosition();

            // schema_name, table_name
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), utf8Slice(tableRow.getSchemaName()));
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), utf8Slice(tableRow.getTableName()));

            // temporal_column
            if (temporalColumnId.isPresent()) {
                if (temporalColumnName == null) {
                    throw new PrestoException(RAPTOR_CORRUPT_METADATA, format("Table ID %s has corrupt metadata (invalid temporal column ID)", tableRow.getTableId()));
                }
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(2), utf8Slice(temporalColumnName));
            }
            else {
                pageBuilder.getBlockBuilder(2).appendNull();
            }

            // ordering_columns
            if (!sortColumnNames.isEmpty()) {
                BlockBuilder orderingColumnsBlockBuilder = pageBuilder.getBlockBuilder(3).beginBlockEntry();
                for (String sortColumnName : sortColumnNames.values()) {
                    VARCHAR.writeSlice(orderingColumnsBlockBuilder, utf8Slice(sortColumnName));
                }
                pageBuilder.getBlockBuilder(3).closeEntry();
            }
            else {
                pageBuilder.getBlockBuilder(3).appendNull();
            }

            // distribution_name
            Optional<String> distributionName = tableRow.getDistributionName();
            if (distributionName.isPresent()) {
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(4), utf8Slice(distributionName.get()));
            }
            else {
                pageBuilder.getBlockBuilder(4).appendNull();
            }

            // bucket_count
            OptionalInt bucketCount = tableRow.getBucketCount();
            if (bucketCount.isPresent()) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), bucketCount.getAsInt());
            }
            else {
                pageBuilder.getBlockBuilder(5).appendNull();
            }

            // bucketing_columns
            if (!bucketColumnNames.isEmpty()) {
                BlockBuilder bucketColumnsBlockBuilder = pageBuilder.getBlockBuilder(6).beginBlockEntry();
                for (String bucketColumnName : bucketColumnNames.values()) {
                    VARCHAR.writeSlice(bucketColumnsBlockBuilder, utf8Slice(bucketColumnName));
                }
                pageBuilder.getBlockBuilder(6).closeEntry();
            }
            else {
                pageBuilder.getBlockBuilder(6).appendNull();
            }

            if (pageBuilder.isFull()) {
                flushPage(pageBuilder, pages);
            }
        }

        flushPage(pageBuilder, pages);
        return pages.build().iterator();
    }

    private static void flushPage(PageBuilder pageBuilder, ImmutableList.Builder<Page> pages)
    {
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    private static int getColumnIndex(ConnectorTableMetadata tableMetadata, String columnName)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(format("Column %s not found", columnName));
    }

    private static String getStringValue(Object value)
    {
        return checkType(value, Slice.class, "value").toStringUtf8();
    }
}
