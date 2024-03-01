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

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock.booleanData;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock.doubleData;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.doubleToLongBits;

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code doubles} array are values for each row. If row is null then value is ignored.
 */
@ThriftStruct
public final class PrestoThriftDouble
        implements PrestoThriftColumnData
{
    private final boolean[] nulls;
    private final double[] doubles;

    @ThriftConstructor
    public PrestoThriftDouble(@Nullable boolean[] nulls, @Nullable double[] doubles)
    {
        checkArgument(sameSizeIfPresent(nulls, doubles), "nulls and values must be of the same size");
        this.nulls = nulls;
        this.doubles = doubles;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public boolean[] getNulls()
    {
        return nulls;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public double[] getDoubles()
    {
        return doubles;
    }

    @Override
    public Block toBlock(Type desiredType)
    {
        checkArgument(DOUBLE.equals(desiredType), "type doesn't match: %s", desiredType);
        int numberOfRecords = numberOfRecords();
        long[] longs = new long[numberOfRecords];
        if (doubles != null) {
            for (int i = 0; i < numberOfRecords; i++) {
                longs[i] = doubleToLongBits(doubles[i]);
            }
        }
        return new LongArrayBlock(
                numberOfRecords,
                Optional.ofNullable(nulls),
                longs);
    }

    @Override
    public int numberOfRecords()
    {
        if (nulls != null) {
            return nulls.length;
        }
        if (doubles != null) {
            return doubles.length;
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
        PrestoThriftDouble other = (PrestoThriftDouble) obj;
        return Arrays.equals(this.nulls, other.nulls) &&
                Arrays.equals(this.doubles, other.doubles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(nulls), Arrays.hashCode(doubles));
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
        int positions = block.getPositionCount();
        if (positions == 0) {
            return booleanData(new PrestoThriftBoolean(null, null));
        }
        boolean[] nulls = null;
        double[] doubles = null;
        for (int position = 0; position < positions; position++) {
            if (block.isNull(position)) {
                if (nulls == null) {
                    nulls = new boolean[positions];
                }
                nulls[position] = true;
            }
            else {
                if (doubles == null) {
                    doubles = new double[positions];
                }
                doubles[position] = DOUBLE.getDouble(block, position);
            }
        }
        return doubleData(new PrestoThriftDouble(nulls, doubles));
    }

    private static boolean sameSizeIfPresent(boolean[] nulls, double[] doubles)
    {
        return nulls == null || doubles == null || nulls.length == doubles.length;
    }
}
