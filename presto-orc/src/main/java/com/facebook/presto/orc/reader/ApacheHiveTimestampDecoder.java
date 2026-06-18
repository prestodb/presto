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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.DecodeTimestampOptions;

final class ApacheHiveTimestampDecoder
{
    private ApacheHiveTimestampDecoder() {}

    // This comes from the Apache Hive ORC code
    public static long decodeTimestamp(long seconds, long serializedNanos, DecodeTimestampOptions options)
    {
        boolean enableMicroPrecision = options.enableMicroPrecision();
        long secondsWithBase = seconds + options.getBaseSeconds();
        long value = getSecondsInRequiredUnits(enableMicroPrecision, secondsWithBase, options.getUnitsPerSecond());
        long nanos = parseNanos(serializedNanos);
        if (nanos > 999999999 || nanos < 0) {
            throw new IllegalArgumentException("nanos field of an encoded timestamp in ORC must be between 0 and 999999999 inclusive, got " + nanos);
        }

        // the rounding error exists because java always rounds up when dividing integers
        // -42001/1000 = -42; and -42001 % 1000 = -1 (+ 1000)
        // to get the correct value we need
        // (-42 - 1)*1000 + 999 = -42001
        // (42)*1000 + 1 = 42001
        if (value < 0 && nanos != 0) {
            value -= options.getUnitsPerSecond();
        }
        // Truncate nanos to required units (millis / micros)
        long truncatedNanos = nanos / options.getNanosPerUnit();
        return getValueWithNanos(enableMicroPrecision, value, truncatedNanos);
    }

    private static long getSecondsInRequiredUnits(boolean enableMicroPrecision, long secondsWithBase, long unitsPerSecond)
    {
        if (!enableMicroPrecision) {
            // This can overflow/underflow, but to maintain backward compatibility this is not bounds checked.
            return secondsWithBase * unitsPerSecond;
        }
        try {
            // Overflow/underflow is detected and the code will raise error.
            return Math.multiplyExact(secondsWithBase, unitsPerSecond);
        }
        catch (ArithmeticException e) {
            String errorMessage = String.format("seconds field of timestamp exceeds maximum supported value, secondsWithBase: %s unitsPerSecond: %s.",
                    secondsWithBase, unitsPerSecond);
            throw new TimestampOutOfBoundsException(errorMessage, e);
        }
    }

    // Add truncated nanos to seconds value
    private static long getValueWithNanos(boolean enableMicroPrecision, long value, long truncatedNanos)
    {
        if (!enableMicroPrecision) {
            // This can overflow/underflow, but to maintain backward compatibility this is not bounds checked.
            return value + truncatedNanos;
        }
        try {
            // Overflow/underflow is detected and the code will raise error.
            return Math.addExact(value, truncatedNanos);
        }
        catch (ArithmeticException e) {
            String errorMessage = String.format("Timestamp exceeds maximum supported value, value: %s truncatedNanos: %s.",
                    value, truncatedNanos);
            throw new TimestampOutOfBoundsException(errorMessage, e);
        }
    }

    // This comes from the Apache Hive ORC code
    private static int parseNanos(long serialized)
    {
        int zeros = ((int) serialized) & 0b111;
        int result = (int) (serialized >>> 3);
        if (zeros != 0) {
            for (int i = 0; i <= zeros; ++i) {
                result *= 10;
            }
        }
        return result;
    }
}
