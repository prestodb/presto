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
package io.prestosql.plugin.hive.util;

import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigInteger;

import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.rescale;

public final class DecimalUtils
{
    private DecimalUtils() {}

    public static long getShortDecimalValue(HiveDecimalWritable writable, int columnScale)
    {
        byte[] bytes = writable.getInternalStorage();
        long value = getShortDecimalValue(bytes);
        value = rescale(value, writable.getScale(), columnScale);
        return value;
    }

    public static long getShortDecimalValue(byte[] bytes)
    {
        long value = 0;
        if ((bytes[0] & 0x80) != 0) {
            for (int i = 0; i < 8 - bytes.length; ++i) {
                value |= 0xFFL << (8 * (7 - i));
            }
        }

        for (int i = 0; i < bytes.length; i++) {
            value |= ((long) bytes[bytes.length - i - 1] & 0xFFL) << (8 * i);
        }

        return value;
    }

    public static Slice getLongDecimalValue(HiveDecimalWritable writable, int columnScale)
    {
        BigInteger value = new BigInteger(writable.getInternalStorage());
        value = rescale(value, writable.getScale(), columnScale);
        return encodeUnscaledValue(value);
    }
}
