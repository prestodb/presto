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

import com.facebook.presto.common.TimestampConstants;

import java.util.Objects;

import static java.lang.String.format;

public final class LongTimestamp
{
    private final long epochMicros;
    private final int picosOfMicro;

    public LongTimestamp(long epochMicros, int picosOfMicro)
    {
        TimestampConstants.checkPicosOfMicro(picosOfMicro);
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
        return format("LongTimestamp{epochMicros=%d, picosOfMicro=%d}", epochMicros, picosOfMicro);
    }
}
