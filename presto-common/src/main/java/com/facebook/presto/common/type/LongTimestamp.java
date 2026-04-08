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
package com.facebook.presto.common.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

import static java.lang.String.format;

/**
 * Represents a timestamp value with precision greater than microseconds (precision 7-12).
 * Stores the epoch microseconds and an additional fractional component in picoseconds
 * of the microsecond (0 to 999999 inclusive).
 * <p>
 * This class is the value type used by {@link LongTimestampType} for precisions 7 through 12.
 */
public final class LongTimestamp
        implements Comparable<LongTimestamp>
{
    private static final int PICOSECONDS_PER_MICROSECOND = 1_000_000;

    private final long epochMicros;
    private final int picosOfMicro;

    public LongTimestamp(long epochMicros, int picosOfMicro)
    {
        if (picosOfMicro < 0 || picosOfMicro >= PICOSECONDS_PER_MICROSECOND) {
            throw new IllegalArgumentException(format("picosOfMicro is out of range: %s", picosOfMicro));
        }
        this.epochMicros = epochMicros;
        this.picosOfMicro = picosOfMicro;
    }

    public long getEpochMicros()
    {
        return epochMicros;
    }

    public int getPicosOfMicro()
    {
        return picosOfMicro;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongTimestamp that = (LongTimestamp) o;
        return epochMicros == that.epochMicros && picosOfMicro == that.picosOfMicro;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epochMicros, picosOfMicro);
    }

    @Override
    public int compareTo(LongTimestamp other)
    {
        int result = Long.compare(epochMicros, other.epochMicros);
        if (result != 0) {
            return result;
        }
        return Integer.compare(picosOfMicro, other.picosOfMicro);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return Timestamps.formatTimestamp(12, epochMicros, picosOfMicro);
    }
}
