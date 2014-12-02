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

import com.facebook.presto.raptor.storage.OutputHandle;
import com.facebook.presto.raptor.storage.RowSink;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.raptor.storage.TupleBuffer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.spi.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RaptorRecordSink
        implements RecordSink
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final String nodeId;
    private final StorageManager storageManager;
    private final List<Type> columnTypes;
    private final OutputHandle outputHandle;
    private final RowSink rowSink;
    private final TupleBuffer tupleBuffer;

    public RaptorRecordSink(
            String nodeId,
            StorageManager storageManager,
            StorageService storageService,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId)
    {
        checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");

        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        this.outputHandle = new OutputHandle(columnIds, columnTypes, storageService);
        this.rowSink = outputHandle.getRowSink();
        this.tupleBuffer = new TupleBuffer(columnTypes, columnIds.indexOf(sampleWeightColumnId.or(-1L)));
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        tupleBuffer.reset();
        tupleBuffer.setSampleWeight(sampleWeight);
    }

    @Override
    public void finishRecord()
    {
        checkState(tupleBuffer.isFinalized());
        rowSink.appendTuple(tupleBuffer);
    }

    @Override
    public void appendNull()
    {
        tupleBuffer.appendNull();
    }

    @Override
    public void appendBoolean(boolean value)
    {
        tupleBuffer.appendBoolean(value);
    }

    @Override
    public void appendLong(long value)
    {
        tupleBuffer.appendLong(getLongValue(currentType(), value));
    }

    @Override
    public void appendDouble(double value)
    {
        tupleBuffer.appendDouble(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        tupleBuffer.appendSlice(Slices.wrappedBuffer(value));
    }

    @Override
    public String commit()
    {
        rowSink.close();
        List<UUID> uuids = storageManager.commit(outputHandle);

        String shardUuids = Joiner.on(',').join(uuids);
        return Joiner.on(':').join(nodeId, shardUuids);
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
            return value / MILLIS_IN_DAY;
        }
        throw unsupportedType(type);
    }

    private Type currentType()
    {
        return columnTypes.get(tupleBuffer.currentField());
    }

    private static PrestoException unsupportedType(Type type)
    {
        return new PrestoException(NOT_SUPPORTED, "Type is not supported for writing: " + type);
    }
}
