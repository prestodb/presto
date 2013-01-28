package com.facebook.presto.hive;

public class NumberParser
{
    public static long parseLong(byte[] bytes, int start, int length)
    {
        int limit = start + length;

        int sign = bytes[start] == '-' ? -1 : 1;

        if (sign == -1 || bytes[start] == '+') {
            start++;
        }

        long value = bytes[start++] - '0';
        while (start < limit) {
            value = value * 10 + (bytes[start] - '0');
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
