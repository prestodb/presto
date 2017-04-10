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
package com.facebook.presto.hive;

import com.facebook.presto.spi.type.DecimalType;

import java.math.BigDecimal;

import static com.facebook.presto.spi.type.Decimals.rescale;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HiveDecimalParser
{
    private HiveDecimalParser() {}

    public static BigDecimal parseHiveDecimal(byte[] bytes, int start, int length, DecimalType columnType)
    {
        BigDecimal parsed = new BigDecimal(new String(bytes, start, length, UTF_8));
        if (parsed.scale() > columnType.getScale()) {
            // Hive rounds HALF_UP too
            parsed = parsed.setScale(columnType.getScale(), HALF_UP);
        }
        return rescale(parsed, columnType);
    }
}
