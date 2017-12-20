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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Functions for reporting values in succinct representations
 */
public final class UnitFunctions
{
    private UnitFunctions()
    {
    }

    @Description("Returns the succinct string representation of a time value")
    @ScalarFunction("succinct_duration")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctDuration(@SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        Duration duration;
        try {
            duration = Duration.succinctDuration(value, valueOfTimeUnit(unit.toStringUtf8()));
        }
        catch (IllegalArgumentException e) {
            // When value is negative, NaN, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(duration.toString());
    }

    @Description("Returns the succinct string representation of a nanosecond value")
    @ScalarFunction("succinct_nanos")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctNanos(@SqlType(StandardTypes.BIGINT) long value)
    {
        return succinctNanos((double) value);
    }

    @Description("Returns the succinct string representation of a nanosecond value")
    @ScalarFunction("succinct_nanos")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctNanos(@SqlType(StandardTypes.DOUBLE) double value)
    {
        Duration duration;
        try {
            duration = Duration.succinctDuration(value, NANOSECONDS);
        }
        catch (IllegalArgumentException e) {
            // When value is negative, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(duration.toString());
    }

    @Description("Returns the succinct string representation of a millisecond value")
    @ScalarFunction("succinct_millis")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctMillis(@SqlType(StandardTypes.BIGINT) long value)
    {
        return succinctMillis((double) value);
    }

    @Description("Returns the succinct string representation of a millisecond value")
    @ScalarFunction("succinct_millis")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctMillis(@SqlType(StandardTypes.DOUBLE) double value)
    {
        Duration duration;
        try {
            duration = Duration.succinctDuration(value, MILLISECONDS);
        }
        catch (IllegalArgumentException e) {
            // When value is negative, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(duration.toString());
    }

    @Description("Returns the succinct string representation of a data size")
    @ScalarFunction("succinct_data_size")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctDataSize(@SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        DataSize dataSize;
        try {
            dataSize = DataSize.succinctDataSize(value, valueOfUnit(unit.toStringUtf8()));
        }
        catch (IllegalArgumentException e) {
            // When value is negative, NaN, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(dataSize.toString());
    }

    @Description("Returns the succinct string representation of a data size specified in bytes")
    @ScalarFunction("succinct_bytes")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctBytes(@SqlType(StandardTypes.BIGINT) long value)
    {
        DataSize dataSize;
        try {
            dataSize = DataSize.succinctBytes(value);
        }
        catch (IllegalArgumentException e) {
            // When value is negative, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(dataSize.toString());
    }

    @Description("Returns the succinct string representation of a data size specified in bytes")
    @ScalarFunction("succinct_bytes")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice succinctBytes(@SqlType(StandardTypes.DOUBLE) double value)
    {
        DataSize dataSize;
        try {
            dataSize = DataSize.succinctDataSize(value, BYTE);
        }
        catch (IllegalArgumentException e) {
            // When value is negative, etc.
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
        return utf8Slice(dataSize.toString());
    }

    private static TimeUnit valueOfTimeUnit(String timeUnitString)
    {
        try {
            return Duration.valueOfTimeUnit(timeUnitString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Time unit must be one of [ns, us, ms, s, m, h, d]", e);
        }
    }

    private static Unit valueOfUnit(String unitString)
    {
        for (Unit unit : Unit.values()) {
            if (unit.getUnitString().equals(unitString)) {
                return unit;
            }
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Unit string must be one of [B, kB, MB, GB, TB, PB]");
    }
}
