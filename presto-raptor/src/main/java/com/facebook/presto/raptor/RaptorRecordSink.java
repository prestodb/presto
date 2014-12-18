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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.RowSink;
import com.facebook.presto.raptor.storage.OutputHandle;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RaptorRecordSink
        implements RecordSink
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final String nodeId;
    private final StorageManager storageManager;
    private final List<Type> columnTypes;
    private final OutputHandle outputHandle;
    private final RowSink rowSink;

    public RaptorRecordSink(
            String nodeId,
            StorageManager storageManager,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.outputHandle = storageManager.createOutputHandle(columnIds, columnTypes, sampleWeightColumnId);
        this.rowSink = outputHandle.getRowSink();
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        rowSink.beginRecord(sampleWeight);
    }

    @Override
    public void finishRecord()
    {
        rowSink.finishRecord();
    }

    @Override
    public void appendNull()
    {
        rowSink.appendNull();
    }

    @Override
    public void appendBoolean(boolean value)
    {
        Type type = currentType();
        if (type.equals(BOOLEAN)) {
            rowSink.appendBoolean(value);
        }
        else {
            throw unsupportedType(type);
        }
    }

    @Override
    public void appendLong(long value)
    {
        rowSink.appendLong(getLongValue(currentType(), value));
    }

    private static long getLongValue(Type type, long value)
    {
        if (type.equals(BIGINT) ||
                type.equals(TIME) ||
                type.equals(TIMESTAMP) ||
                type.equals(INTERVAL_YEAR_MONTH) ||
                type.equals(INTERVAL_DAY_TIME)) {
            return value;
        }
        if (type.equals(DATE)) {
            return value;
        }
        throw unsupportedType(type);
    }

    @Override
    public void appendDouble(double value)
    {
        Type type = currentType();
        if (type.equals(DOUBLE)) {
            rowSink.appendDouble(value);
        }
        else {
            throw unsupportedType(type);
        }
    }

    @Override
    public void appendString(byte[] value)
    {
        Type type = currentType();
        if (type.equals(VARCHAR)) {
            rowSink.appendString(new String(value, UTF_8));
        }
        else if (type.equals(VARBINARY)) {
            rowSink.appendBytes(value);
        }
        else {
            throw unsupportedType(type);
        }
    }

    @Override
    public String commit()
    {
        storageManager.commit(outputHandle);

        return Joiner.on(':').join(nodeId, outputHandle.getShardUuid());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private Type currentType()
    {
        return columnTypes.get(rowSink.currentField());
    }

    private static PrestoException unsupportedType(Type type)
    {
        return new PrestoException(NOT_SUPPORTED, "Type is not supported for writing: " + type);
    }
}
