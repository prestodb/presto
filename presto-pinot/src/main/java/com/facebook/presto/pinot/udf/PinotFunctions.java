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
package com.facebook.presto.pinot.udf;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import org.apache.commons.codec.binary.Hex;

import java.math.BigDecimal;
import java.math.BigInteger;

public class PinotFunctions
{
    private PinotFunctions() {}

    @Description("pinot binary decimal to double")
    @ScalarFunction(value = "pinot_binary_decimal_to_double")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE) // TODO: use StandardTypes.DECIMAL
    public static Double pinotBinaryDecimalToDouble(
            @SqlType(StandardTypes.VARBINARY) Slice input,
            @SqlType(StandardTypes.INTEGER) long bigIntegerRadix,
            @SqlType(StandardTypes.INTEGER) long scale,
            @SqlType(StandardTypes.BOOLEAN) boolean returnZeroOnNull)
    {
        if (input == null || input.getBytes().length == 0) {
            if (returnZeroOnNull) {
                return 0D;
            }
            else {
                return null;
            }
        }
        return new BigDecimal(new BigInteger(Hex.encodeHexString(input.getBytes()),
                (int) bigIntegerRadix), (int) scale).doubleValue();
//        return Decimals.encodeScaledValue(new BigDecimal(new BigInteger(hex, (int) bigIntegerRadix),
//                (int) scale), (int) scale);
    }
}
