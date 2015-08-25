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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import jodd.datetime.JDateTime;
import parquet.io.api.Binary;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Utility class for decoding INT96 encoded parquet timestamp to timestamp millis in GMT.
 *
 * This class is equivalent of @see org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime,
 * which produces less intermediate object during decoding.
 */
public final class ParquetTimestampUtils
{
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final ThreadLocal<JDateTime> DATE_TIME_THREAD_LOCAL = new ThreadLocal<JDateTime>()
    {
        @Override
        protected JDateTime initialValue()
        {
            JDateTime dateTime = new JDateTime();
            dateTime.setTimeZone(TimeZone.getTimeZone("GMT"));
            return dateTime;
        }
    };

    /**
     * Returns GMT timestamp from binary encoded parquet timestamp (12 bytes - julian date + time of day nanos).
     *
     * @param timestampBinary INT96 parquet timestamp
     * @return timestamp in millis, GMT timezone
     */
    public static long getTimestampMillis(Binary timestampBinary)
    {
        checkArgument(timestampBinary.length() == 12, "Parquet timestamp must be 12 bytes long");
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        JDateTime dateTime = DATE_TIME_THREAD_LOCAL.get();
        dateTime.setJulianDate(julianDay);
        dateTime.setTime(0, 0, 0, 0);

        return dateTime.getTimeInMillis() + timeOfDayNanos / NANOS_PER_MILLISECOND;
    }

    private ParquetTimestampUtils()
    {
    }
}
