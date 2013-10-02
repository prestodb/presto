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

public final class NumberParser
{
    private NumberParser() {}

    public static long parseLong(byte[] bytes, int start, int length)
    {
        int limit = start + length;

        int sign = bytes[start] == '-' ? -1 : 1;

        if (sign == -1 || bytes[start] == '+') {
            start++;
        }

        long value = bytes[start] - ((int) '0');
        start++;
        while (start < limit) {
            value = value * 10 + (bytes[start] - ((int) '0'));
            start++;
        }

        return value * sign;
    }

    public static double parseDouble(byte[] bytes, int start, int length)
    {
        char[] chars = new char[length];
        for (int pos = 0; pos < length; pos++) {
            chars[pos] = (char) bytes[start + pos];
        }
        String string = new String(chars);
        return Double.parseDouble(string);
    }
}
