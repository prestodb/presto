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
package io.prestosql.plugin.hive;

public final class HiveBooleanParser
{
    private HiveBooleanParser() {}

    public static Boolean parseHiveBoolean(byte[] bytes, int start, int length)
    {
        if (isTrue(bytes, start, length)) {
            return true;
        }
        if (isFalse(bytes, start, length)) {
            return false;
        }
        return null;
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    public static boolean isFalse(byte[] bytes, int start, int length)
    {
        return (length == 5) &&
                (toUpperCase(bytes[start + 0]) == 'F') &&
                (toUpperCase(bytes[start + 1]) == 'A') &&
                (toUpperCase(bytes[start + 2]) == 'L') &&
                (toUpperCase(bytes[start + 3]) == 'S') &&
                (toUpperCase(bytes[start + 4]) == 'E');
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    public static boolean isTrue(byte[] bytes, int start, int length)
    {
        return (length == 4) &&
                (toUpperCase(bytes[start + 0]) == 'T') &&
                (toUpperCase(bytes[start + 1]) == 'R') &&
                (toUpperCase(bytes[start + 2]) == 'U') &&
                (toUpperCase(bytes[start + 3]) == 'E');
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }
}
