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

package com.facebook.presto.hive.functions.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.common.type.HiveDecimal;

import java.math.BigDecimal;

import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.Decimals.readBigDecimal;
import static com.facebook.presto.common.type.Decimals.rescale;

public final class DecimalUtils
{
    private DecimalUtils() {}

    public static long encodeToLong(BigDecimal decimal, DecimalType type)
    {
        return rescale(decimal, type).unscaledValue().longValue();
    }

    public static Slice encodeToSlice(BigDecimal decimal, DecimalType type)
    {
        return encodeScaledValue(rescale(decimal, type));
    }

    public static HiveDecimal readHiveDecimal(DecimalType type, Block block, int position)
    {
        BigDecimal bigDecimal = readBigDecimal(type, block, position);
        return HiveDecimal.create(bigDecimal);
    }
}
