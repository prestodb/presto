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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CassandraPageSink
        implements ConnectorPageSink
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final CassandraSession cassandraSession;
    private final PreparedStatement insert;
    private final List<Type> columnTypes;
    private final boolean generateUUID;

    public CassandraPageSink(
            CassandraSession cassandraSession,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            boolean generateUUID)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.generateUUID = generateUUID;

        Insert insert = insertInto(schemaName, tableName);
        if (generateUUID) {
            insert.value("id", bindMarker());
        }
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            checkArgument(columnName != null, "columnName is null at position: %d", i);
            insert.value(columnName, bindMarker());
        }
        this.insert = cassandraSession.prepare(insert);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(columnTypes.size() + 1);
            if (generateUUID) {
                values.add(UUID.randomUUID());
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(values, page, position, channel);
            }

            cassandraSession.execute(insert.bind(values.toArray()));
        }
        return NOT_BLOCKED;
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
        else if (DOUBLE.equals(type)) {
            values.add(type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            values.add(intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (DATE.equals(type)) {
            values.add(DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(type.getLong(block, position))));
        }
        else if (TIMESTAMP.equals(type)) {
            values.add(new Timestamp(type.getLong(block, position)));
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
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}
