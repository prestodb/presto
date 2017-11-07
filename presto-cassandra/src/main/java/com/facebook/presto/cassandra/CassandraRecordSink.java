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
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CassandraRecordSink
        implements RecordSink
{
    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    private final CassandraSession cassandraSession;
    private final PreparedStatement insert;
    private final List<Object> values;
    private final List<Type> columnTypes;
    private final boolean generateUUID;

    private int field = -1;

    public CassandraRecordSink(
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

        values = new ArrayList<>(columnTypes.size() + 1);
    }

    @Override
    public void beginRecord()
    {
        checkState(field == -1, "already in record");

        field = 0;
        values.clear();
        if (generateUUID) {
            values.add(UUID.randomUUID());
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == columnTypes.size(), "not all fields set");
        field = -1;
        cassandraSession.execute(insert.bind(values.toArray()));
    }

    @Override
    public void appendNull()
    {
        append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    public void appendLong(long value)
    {
        Type columnType = columnTypes.get(field);
        if (DATE.equals(columnType)) {
            append(DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(value)));
        }
        else if (INTEGER.equals(columnType)) {
            append(((Number) value).intValue());
        }
        else if (REAL.equals(columnType)) {
            append(intBitsToFloat((int) value));
        }
        else if (TIMESTAMP.equals(columnType)) {
            append(new Timestamp(value));
        }
        else if (BIGINT.equals(columnType)) {
            append(value);
        }
        else {
            throw new UnsupportedOperationException("Type is not supported: " + columnType);
        }
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendBigDecimal(BigDecimal value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendString(byte[] value)
    {
        Type columnType = columnTypes.get(field);
        if (VARBINARY.equals(columnType)) {
            append(ByteBuffer.wrap(value));
        }
        else if (isVarcharType(columnType)) {
            append(new String(value, UTF_8));
        }
        else {
            throw new UnsupportedOperationException("Type is not supported: " + columnType);
        }
    }

    @Override
    public void appendObject(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Slice> commit()
    {
        checkState(field == -1, "record not finished");
        // the committer does not need any additional info
        return ImmutableList.of();
    }

    @Override
    public void rollback() {}

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < columnTypes.size(), "all fields already set");
        values.add(value);
        field++;
    }
}
