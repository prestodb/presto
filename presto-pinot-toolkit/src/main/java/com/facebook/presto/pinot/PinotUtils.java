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
package com.facebook.presto.pinot;

import org.apache.pinot.spi.utils.TimestampUtils;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_MULT_CHOICE;
import static java.net.HttpURLConnection.HTTP_OK;

public class PinotUtils
{
    private static final String PINOT_INFINITY = "∞";
    private static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    private static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;
    private static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    private static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    private PinotUtils()
    {
    }

    static boolean isValidPinotHttpResponseCode(int status)
    {
        return status >= HTTP_OK && status < HTTP_MULT_CHOICE;
    }

    public static <T> T doWithRetries(int retries, Function<Integer, T> caller)
    {
        PinotException firstError = null;
        checkState(retries > 0, "Invalid num of retries %d", retries);
        for (int i = 0; i < retries; ++i) {
            try {
                return caller.apply(i);
            }
            catch (PinotException e) {
                if (firstError == null) {
                    firstError = e;
                }
                if (!e.getPinotErrorCode().isRetriable()) {
                    throw e;
                }
            }
        }
        throw firstError;
    }

    public static long parseTimestamp(String value)
    {
        try {
            return parseLong(value);
        }
        catch (NumberFormatException e) {
            // Sometimes Pinot returns float point string in a field that we expect timestamp.
            // This can happen because of JsonParser, Pinot Query Engine, etc.
            // In this case we may still go through by reading a float value
            try {
                return parseDouble(value).longValue();
            }
            catch (Exception ignoredEx) {
                return TimestampUtils.toMillisSinceEpoch(value);
            }
        }
    }

    public static Double parseDouble(String value)
    {
        try {
            return Double.valueOf(value);
        }
        catch (NumberFormatException ne) {
            switch (value) {
                case PINOT_INFINITY:
                case PINOT_POSITIVE_INFINITY:
                    return PRESTO_INFINITY;
                case PINOT_NEGATIVE_INFINITY:
                    return PRESTO_NEGATIVE_INFINITY;
            }
            throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), "Cannot decode double value from pinot " + value, ne);
        }
    }
}
