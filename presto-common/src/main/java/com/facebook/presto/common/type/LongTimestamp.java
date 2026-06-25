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

import java.util.Objects;

// Value type for timestamps with precision > 6. Two fields pack 12 bytes:
// - epochMicros (8 bytes): microseconds since 1970-01-01T00:00:00 UTC, negative for pre-1970
// - picosOfMicro (4 bytes): sub-microsecond remainder, always in [0, 999_999]
public final class LongTimestamp
{
    public static final int MAX_PICOS_OF_MICRO = 999_999;

    private final long epochMicros;
    private final int picosOfMicro;

    public LongTimestamp(long epochMicros, int picosOfMicro)
    {
        if (picosOfMicro < 0 || picosOfMicro > MAX_PICOS_OF_MICRO) {
            throw new IllegalArgumentException(
                    "picosOfMicro must be in [0, " + MAX_PICOS_OF_MICRO + "]: " + picosOfMicro);
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
    public String toString()
    {
        return "LongTimestamp{epochMicros=" + epochMicros + ", picosOfMicro=" + picosOfMicro + "}";
    }
}
