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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;

public final class FormatFunctions
{
    private FormatFunctions() {}

    @Description("format to human readable count value")
    @ScalarFunction("format_count")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatCount(@SqlType(StandardTypes.BIGINT) long num)
    {
        checkCondition(num != Long.MIN_VALUE, NUMERIC_VALUE_OUT_OF_RANGE, "Value -9223372036854775808 is out of range for abs(bigint)");
        return Slices.utf8Slice(fmtCount((double) num));
    }

    @Description("format to human readable count value")
    @ScalarFunction("format_count")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatCountDouble(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Slices.utf8Slice(fmtCount(num));
    }

    public static String fmtCount(double num)
    {
        double fractional = num;
        String unit = "";
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "K";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "M";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "B";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "T";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "Q";
        }

        return format("%s%s", getFormat(fractional).format(fractional), unit);
    }

    private static DecimalFormat getFormat(double value)
    {
        DecimalFormat format;
        if (value < 10) {
            // show up to two decimals to get 3 significant digits
            format = new DecimalFormat("#.##");
        }
        else if (value < 100) {
            // show up to one decimal to get 3 significant digits
            format = new DecimalFormat("#.#");
        }
        else {
            // show no decimals -- we have enough digits in the integer part
            format = new DecimalFormat("#");
        }

        format.setRoundingMode(RoundingMode.HALF_UP);
        return format;
    }
}
