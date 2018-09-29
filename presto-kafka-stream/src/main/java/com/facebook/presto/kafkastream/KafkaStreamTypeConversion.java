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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Utility class to convert value to presto data types.
 */
// Extend this class if adapter specific conversion is needed.
public class KafkaStreamTypeConversion
{
    private static final String EPOCH_TIME_REGEX = "\\d{13}";
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    public static boolean toPrestoBoolean(Object value)
    {
        return value instanceof String ? Boolean.parseBoolean((String) value) : (Boolean) value;
    }

    public static double toPrestoDouble(Object value)
    {
        return value instanceof String ? Double.parseDouble((String) value) : (Double) value;
    }

    public static int toPrestoInteger(Object value)
    {
        return value instanceof String ? Integer.parseInt((String) value) : (Integer) value;
    }

    public static long toPrestoBigint(Object value)
    {
        return value instanceof String ? Long.parseLong((String) value) : (Long) value;
    }

    public static Slice toPrestoString(Object value)
    {
        return Slices.utf8Slice((String) value);
    }

    private KafkaStreamTypeConversion()
    {
    }

    public static long toPrestoDate(Object value)
    {
        try {
            if (value instanceof String) {
                String valueStr = (String) value;
                if (valueStr.matches(EPOCH_TIME_REGEX)) {
                    return TimeUnit.MILLISECONDS.toDays(Long.parseLong(valueStr));
                }
                else {
                    DateFormat sourceFormat = new SimpleDateFormat(DATE_FORMAT);
                    sourceFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                    Date date = sourceFormat.parse(valueStr);
                    return TimeUnit.MILLISECONDS.toDays(date.getTime());
                }
            }
            else {
                Date date = (Date) value;
                return TimeUnit.MILLISECONDS.toDays(date.getTime());
            }
        }
        catch (ParseException exp) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, exp);
        }
    }

    public static long toPrestoTimeStampWithTimezone(Object value)
    {
        try {
            if (value instanceof String) {
                String valueStr = (String) value;
                // if string value is in epoch time
                if (valueStr.matches(EPOCH_TIME_REGEX)) {
                    return Long.parseLong(valueStr);
                }
                else {
                    DateFormat sourceFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
                    Date date = sourceFormat.parse(valueStr);
                    return date.getTime();
                }
            }
            else {
                Date date = (Date) value;
                return date.getTime();
            }
        }
        catch (ParseException exp) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, exp);
        }
    }

    public static <T> T fetchValueFromCursor(RecordCursor cursor, int fieldIndex, Class<T> javaType)
    {
        Object value;
        if (javaType == boolean.class || javaType == Boolean.class) {
            value = cursor.getBoolean(fieldIndex);
        }
        else if (javaType == long.class || javaType == Long.class) {
            value = cursor.getLong(fieldIndex);
        }
        else if (javaType == double.class || javaType == Double.class) {
            value = cursor.getDouble(fieldIndex);
        }
        else if (javaType == Slice.class) {
            value = cursor.getSlice(fieldIndex);
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for " + javaType.getSimpleName());
        }
        return (T) value;
    }
}
