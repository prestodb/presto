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
package com.facebook.presto.hive.util;

import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.ShortDecimalType.parseShortDecimalBytes;
import static com.google.common.base.Preconditions.checkState;

public final class DecimalUtils
{
    private DecimalUtils() {}

    public static long getShortDecimalValue(HiveDecimalWritable writable, int columnScale)
    {
        byte[] bytes = writable.getInternalStorage();
        long value = parseShortDecimalBytes(bytes);
        value = rescale(value, writable.getScale(), columnScale);
        return value;
    }

    public static Slice getLongDecimalValue(HiveDecimalWritable writable, int columnScale)
    {
        BigInteger value = writable.getHiveDecimal().unscaledValue();
        return LongDecimalType.unscaledValueToSlice(rescale(value, writable.getScale(), columnScale));
    }

    public static long rescale(long value, int fromScale, int toScale)
    {
        checkState(fromScale <= toScale, "target scale must be larger than source scale");
        return value * ShortDecimalType.tenToNth(toScale - fromScale);
    }

    public static BigInteger rescale(BigInteger value, int fromScale, int toScale)
    {
        checkState(fromScale <= toScale, "target scale must be larger than source scale");
        return value.multiply(LongDecimalType.tenToNth(toScale - fromScale));
    }
}
