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
// Copyright 2015 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.facebook.presto.druid.zip;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.zip.ZipException;

/**
 * A utility class for reading and writing {@link ZipFileEntry}s from byte arrays.
 */
public class ZipUtil
{
    private ZipUtil()
    {
    }

    /**
     * Midnight Jan 1st 1980. Uses the current time zone as the DOS format does not support time zones
     * and will always assume the current zone.
     */
    public static final long DOS_EPOCH = new GregorianCalendar(1980, Calendar.JANUARY, 1, 0, 0, 0).getTimeInMillis();

    /**
     * 23:59:59 Dec 31st 2107. The maximum date representable in DOS format.
     */
    public static final long MAX_DOS_DATE = new GregorianCalendar(2107, Calendar.DECEMBER, 31, 23, 59, 59).getTimeInMillis();

    /* DOS format timestamp field offsets. */
    private static final int DOS_MINUTE_OFFSET = 5;
    private static final int DOS_HOUR_OFFSET = 11;
    private static final int DOS_DAY_OFFSET = 16;
    private static final int DOS_MONTH_OFFSET = 21;
    private static final int DOS_YEAR_OFFSET = 25;

    private static byte[] integerToLittleEndian(byte[] buf, int offset, long value, int numBytes)
    {
        for (int i = 0; i < numBytes; i++) {
            buf[i + offset] = (byte) ((value & (0xffL << (i * 8))) >> (i * 8));
        }
        return buf;
    }

    public static byte[] shortToLittleEndian(short value)
    {
        return integerToLittleEndian(new byte[2], 0, value, 2);
    }

    public static byte[] shortToLittleEndian(byte[] buf, int offset, short value)
    {
        return integerToLittleEndian(buf, offset, value, 2);
    }

    public static byte[] intToLittleEndian(int value)
    {
        return integerToLittleEndian(new byte[4], 0, value, 4);
    }

    public static byte[] intToLittleEndian(byte[] buf, int offset, int value)
    {
        return integerToLittleEndian(buf, offset, value, 4);
    }

    public static byte[] longToLittleEndian(long value)
    {
        return integerToLittleEndian(new byte[8], 0, value, 8);
    }

    public static byte[] longToLittleEndian(byte[] buf, int offset, long value)
    {
        return integerToLittleEndian(buf, offset, value, 8);
    }

    public static short get16(byte[] source, int offset)
    {
        int a = source[offset + 0] & 0xff;
        int b = source[offset + 1] & 0xff;
        return (short) ((b << 8) | a);
    }

    public static int get32(byte[] source, int offset)
    {
        int a = source[offset + 0] & 0xff;
        int b = source[offset + 1] & 0xff;
        int c = source[offset + 2] & 0xff;
        int d = source[offset + 3] & 0xff;
        return (d << 24) | (c << 16) | (b << 8) | a;
    }

    public static long get64(byte[] source, int offset)
    {
        long a = source[offset + 0] & 0xffL;
        long b = source[offset + 1] & 0xffL;
        long c = source[offset + 2] & 0xffL;
        long d = source[offset + 3] & 0xffL;
        long e = source[offset + 4] & 0xffL;
        long f = source[offset + 5] & 0xffL;
        long g = source[offset + 6] & 0xffL;
        long h = source[offset + 7] & 0xffL;
        return (h << 56) | (g << 48) | (f << 40) | (e << 32) | (d << 24) | (c << 16) | (b << 8) | a;
    }

    public static int getUnsignedShort(byte[] source, int offset)
    {
        return get16(source, offset) & 0xffff;
    }

    public static long getUnsignedInt(byte[] source, int offset)
    {
        return get32(source, offset) & 0xffffffffL;
    }

    public static long getUnsignedLong(byte[] source, int offset)
            throws ZipException
    {
        long result = get64(source, offset);
        if (result < 0) {
            throw new ZipException("The requested unsigned long value is too large for Java's signed"
                    + "values. This Zip file is unsupported");
        }
        return result;
    }

    /**
     * Checks if the unix timestamp is representable as a valid DOS timestamp.
     *
     * <p>See <a href="http://www.pkware.com/documents/casestudies/APPNOTE.TXT">ZIP Format</a> for
     * a general description of the date a time fields (Section 4.4.6) and
     * <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/ms724247.aspx">DOS date
     * format</a> for a detailed description of the format.
     */
    public static boolean isValidInDos(long timeMillis)
    {
        Calendar time = Calendar.getInstance();
        time.setTimeInMillis(timeMillis);
        Calendar minTime = Calendar.getInstance();
        minTime.setTimeInMillis(DOS_EPOCH);
        Calendar maxTime = Calendar.getInstance();
        maxTime.setTimeInMillis(MAX_DOS_DATE);
        return (!time.before(minTime) && !time.after(maxTime));
    }

    /**
     * Converts a 32-bit DOS timestamp into a unix timestamp.
     */
    public static long dosToUnixTime(int timestamp)
    {
        Calendar time = Calendar.getInstance();
        time.clear();
        time.set(Calendar.SECOND, (timestamp & 0x1f) * 2);
        time.set(Calendar.MINUTE, (timestamp >> DOS_MINUTE_OFFSET) & 0x3f);
        time.set(Calendar.HOUR_OF_DAY, (timestamp >> DOS_HOUR_OFFSET) & 0x1f);
        time.set(Calendar.DAY_OF_MONTH, (timestamp >> DOS_DAY_OFFSET) & 0x1f);
        time.set(Calendar.MONTH, ((timestamp >> DOS_MONTH_OFFSET) & 0x0f) - 1);
        time.set(Calendar.YEAR, ((timestamp >> DOS_YEAR_OFFSET) & 0x7f) + 1980);
        return time.getTimeInMillis();
    }

    public static boolean arrayStartsWith(byte[] array, byte[] target)
    {
        if (array == null) {
            return false;
        }
        if (target == null) {
            return true;
        }
        if (target.length > array.length) {
            return false;
        }
        for (int i = 0; i < target.length; i++) {
            if (array[i] != target[i]) {
                return false;
            }
        }
        return true;
    }
}
