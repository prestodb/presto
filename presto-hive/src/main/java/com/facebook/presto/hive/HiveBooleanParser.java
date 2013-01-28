package com.facebook.presto.hive;

public class HiveBooleanParser
{
    @SuppressWarnings("PointlessArithmeticExpression")
    public static Boolean parseHiveBoolean(byte[] bytes, int start, int length)
    {
        if ((length == 4) &&
                (toUpperCase(bytes[start + 0]) == 'T') &&
                (toUpperCase(bytes[start + 1]) == 'R') &&
                (toUpperCase(bytes[start + 2]) == 'U') &&
                (toUpperCase(bytes[start + 3]) == 'E')) {
            return true;
        }
        if ((length == 5) &&
                (toUpperCase(bytes[start + 0]) == 'F') &&
                (toUpperCase(bytes[start + 1]) == 'A') &&
                (toUpperCase(bytes[start + 2]) == 'L') &&
                (toUpperCase(bytes[start + 3]) == 'S') &&
                (toUpperCase(bytes[start + 4]) == 'E')) {
            return false;
        }
        return null;
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
