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
package com.facebook.presto.thrift.api.datatypes;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordSet;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock.timestampData;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftTypeUtils.fromLongBasedBlock;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftTypeUtils.fromLongBasedColumn;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code timestamps} array are values for each row represented as the number
 * of milliseconds passed since 1970-01-01T00:00:00 UTC.
 * If row is null then value is ignored.
 */
@ThriftStruct
public final class PrestoThriftTimestamp
        implements PrestoThriftColumnData
{
    private final boolean[] nulls;
    private final long[] timestamps;

    @ThriftConstructor
    public PrestoThriftTimestamp(
            @ThriftField(name = "nulls") @Nullable boolean[] nulls,
            @ThriftField(name = "timestamps") @Nullable long[] timestamps)
    {
        checkArgument(sameSizeIfPresent(nulls, timestamps), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.timestamps = timestamps;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public long[] getTimestamps()
    {
        return timestamps;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(TIMESTAMP.equals(desiredType), "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        return new LongArrayBlock(
                numberOfRecords,
                Optional.ofNullable(nulls),
                timestamps == null ? new long[numberOfRecords] : timestamps);
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (timestamps != null) {
            return timestamps.length;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PrestoThriftTimestamp other = (PrestoThriftTimestamp) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.timestamps, other.timestamps);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(timestamps));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRecords", numberOfRecords())
                .toString();
    }

    public static PrestoThriftBlock fromBlock(Block block)
    {
        return fromLongBasedBlock(block, TIMESTAMP, (nulls, longs) -> timestampData(new PrestoThriftTimestamp(nulls, longs)));
    }

    public static PrestoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        return fromLongBasedColumn(recordSet, columnIndex, totalRecords, (nulls, longs) -> timestampData(new PrestoThriftTimestamp(nulls, longs)));
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, long[] timestamps)
    {
        return nulls == null || timestamps == null || nulls.length == timestamps.length;
    }
}
