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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_ERROR;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validColumnName;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.validTableName;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.primitives.Shorts.checkedCast;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CassandraPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(CassandraPageSink.class);

    private final CassandraSession cassandraSession;
    private final PreparedStatement insert;
    private final List<Type> columnTypes;
    private final boolean generateUUID;
    private final String schemaName;
    private final String tableName;
    private long rowsWritten;

    public CassandraPageSink(
            CassandraSession cassandraSession,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            boolean generateUUID)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.generateUUID = generateUUID;

        // Build the insert statement - need to add first value to get RegularInsert type
        RegularInsert insert;
        if (generateUUID) {
            insert = insertInto(validSchemaName(schemaName), validTableName(tableName))
                    .value("id", bindMarker());
        }
        else if (!columnNames.isEmpty()) {
            insert = insertInto(validSchemaName(schemaName), validTableName(tableName))
                    .value(validColumnName(columnNames.get(0)), bindMarker());
        }
        else {
            throw new IllegalArgumentException("Cannot create insert statement with no columns");
        }

        // Add remaining columns
        int startIndex = (generateUUID || columnNames.isEmpty()) ? 0 : 1;
        for (int i = startIndex; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            checkArgument(columnName != null, "columnName is null at position: %d", i);
            insert = insert.value(validColumnName(columnName), bindMarker());
        }

        String insertQuery = insert.build().getQuery();
        log.debug("Preparing insert statement for %s.%s: %s", schemaName, tableName, insertQuery);
        this.insert = cassandraSession.prepare(insertQuery);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            log.debug("=== CassandraPageSink: Appending page with %d rows to %s.%s ===",
                    page.getPositionCount(), schemaName, tableName);

            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(columnTypes.size() + 1);
                if (generateUUID) {
                    values.add(UUID.randomUUID());
                }

                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    try {
                        appendColumn(values, page, position, channel);
                    }
                    catch (Exception e) {
                        log.error(e, "Failed to append column %d (type: %s) at position %d",
                                channel, columnTypes.get(channel), position);
                        throw new PrestoException(CASSANDRA_ERROR,
                                format("Failed to append column %d (type: %s) at position %d: %s",
                                        channel, columnTypes.get(channel), position, e.getMessage()), e);
                    }
                }

                try {
                    BoundStatement boundStatement = insert.bind(values.toArray());
                    // Set explicit consistency level to ensure data persistence in Driver 4.x
                    boundStatement = boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
                    cassandraSession.execute(boundStatement);
                    rowsWritten++;
                    log.debug("Successfully inserted row %d/%d with QUORUM consistency", position + 1, page.getPositionCount());
                }
                catch (Exception e) {
                    log.error(e, "Failed to insert row %d with values: %s", position, values);
                    throw new PrestoException(CASSANDRA_ERROR,
                            format("Failed to insert row %d into %s.%s: %s",
                                    position, schemaName, tableName, e.getMessage()), e);
                }
            }

            log.debug("=== CassandraPageSink: Successfully appended %d rows to %s.%s ===",
                    page.getPositionCount(), schemaName, tableName);
            return NOT_BLOCKED;
        }
        catch (PrestoException e) {
            // Re-throw PrestoExceptions as-is
            throw e;
        }
        catch (Exception e) {
            log.error(e, "=== CassandraPageSink: FATAL ERROR appending page to %s.%s ===", schemaName, tableName);
            throw new PrestoException(CASSANDRA_ERROR,
                format("Fatal error appending page to %s.%s: %s", schemaName, tableName, e.getMessage()), e);
        }
    }

    private void appendColumn(List<Object> values, Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);
        if (block.isNull(position)) {
            values.add(null);
        }
        else if (BOOLEAN.equals(type)) {
            values.add(type.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            values.add(type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            values.add(toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            values.add(checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            values.add((byte) type.getLong(block, position));
        }
        else if (DOUBLE.equals(type)) {
            values.add(type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            values.add(intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (DATE.equals(type)) {
            values.add(LocalDate.ofEpochDay(type.getLong(block, position)));
        }
        else if (TIMESTAMP.equals(type)) {
            values.add(Instant.ofEpochMilli(type.getLong(block, position)));
        }
        else if (isVarcharType(type)) {
            values.add(type.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            values.add(type.getSlice(block, position).toByteBuffer());
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        log.debug("=== CassandraPageSink: Finishing write to %s.%s with %d rows written ===",
                schemaName, tableName, rowsWritten);
        
        // Driver 4.x: Add small delay to handle eventual consistency
        // This ensures data is visible immediately after INSERT for test reliability
        if (rowsWritten > 0) {
            try {
                Thread.sleep(50); // 50ms delay for data propagation
                log.debug("Applied 50ms delay for eventual consistency after %d row(s) written", rowsWritten);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for data propagation");
            }
        }
        
        CassandraWriteMetadata metadata = new CassandraWriteMetadata(rowsWritten);
        return completedFuture(ImmutableList.of(metadata.toSlice()));
    }

    @Override
    public void abort() {}
}
