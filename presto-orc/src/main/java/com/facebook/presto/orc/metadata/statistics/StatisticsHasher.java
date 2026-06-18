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
package com.facebook.presto.orc.metadata.statistics;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class StatisticsHasher
{
    private final Hasher hasher = Hashing.goodFastHash(64).newHasher();

    public StatisticsHasher putByte(byte value)
    {
        hasher.putByte(value);
        return this;
    }

    public StatisticsHasher putBytes(byte[] bytes)
    {
        hasher.putBytes(bytes);
        return this;
    }

    public StatisticsHasher putBytes(byte[] bytes, int off, int len)
    {
        hasher.putBytes(bytes, off, len);
        return this;
    }

    public StatisticsHasher putBytes(ByteBuffer bytes)
    {
        hasher.putBytes(bytes);
        return this;
    }

    public StatisticsHasher putShort(short value)
    {
        hasher.putShort(value);
        return this;
    }

    public StatisticsHasher putInt(int value)
    {
        hasher.putInt(value);
        return this;
    }

    public StatisticsHasher putOptionalInt(boolean present, int value)
    {
        hasher.putBoolean(present);
        hasher.putInt(present ? value : 0);
        return this;
    }

    public StatisticsHasher putLong(long value)
    {
        hasher.putLong(value);
        return this;
    }

    public StatisticsHasher putOptionalLong(boolean present, long value)
    {
        hasher.putBoolean(present);
        hasher.putLong(present ? value : 0);
        return this;
    }

    public StatisticsHasher putFloat(float value)
    {
        hasher.putFloat(value);
        return this;
    }

    public StatisticsHasher putDouble(double value)
    {
        hasher.putDouble(value);
        return this;
    }

    public StatisticsHasher putOptionalDouble(boolean present, double value)
    {
        hasher.putBoolean(present);
        hasher.putDouble(present ? value : 0);
        return this;
    }

    public StatisticsHasher putBoolean(boolean value)
    {
        hasher.putBoolean(value);
        return this;
    }

    public StatisticsHasher putOptionalHashable(Hashable value)
    {
        hasher.putBoolean(value != null);
        if (value != null) {
            value.addHash(this);
        }
        return this;
    }

    public StatisticsHasher putOptionalSlice(Slice value)
    {
        hasher.putBoolean(value != null);
        if (value != null) {
            // there are better ways to do this, but values are limited to 64 bytes
            hasher.putBytes(value.getBytes());
        }
        return this;
    }

    public StatisticsHasher putOptionalBigDecimal(BigDecimal value)
    {
        hasher.putBoolean(value != null);
        if (value != null) {
            // this should really be 128 bits
            hasher.putInt(value.scale());
            hasher.putBytes(value.unscaledValue().toByteArray());
        }
        return this;
    }

    public long hash()
    {
        return hasher.hash().asLong();
    }

    public void putLongs(long[] array)
    {
        hasher.putInt(array.length);
        for (long entry : array) {
            hasher.putLong(entry);
        }
    }

    public interface Hashable
    {
        void addHash(StatisticsHasher hasher);
    }
}
