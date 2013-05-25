/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.sql.gen;

import com.facebook.presto.operator.scalar.MathFunctions;
import com.google.common.base.Charsets;
import io.airlift.slice.Slice;

public final class Operations
{
    private Operations()
    {
    }


    public static boolean and(boolean left, boolean right)
    {
        return left && right;
    }

    public static boolean or(boolean left, boolean right)
    {
        return left || right;
    }

    public static boolean not(boolean value)
    {
        return !value;
    }

    public static boolean equal(boolean left, boolean right)
    {
        return left == right;
    }

    public static boolean notEqual(boolean left, boolean right)
    {
        return left != right;
    }

    public static boolean equal(long left, long right)
    {
        return left == right;
    }

    public static long add(long left, long right)
    {
        return left + right;
    }

    public static long subtract(long left, long right)
    {
        return left - right;
    }

    public static long multiply(long left, long right)
    {
        return left * right;
    }

    public static long divide(long left, long right)
    {
        return left / right;
    }

    public static long modulus(long left, long right)
    {
        return left % right;
    }

    public static long negate(long value)
    {
        return -value;
    }

    public static double add(double left, double right)
    {
        return left + right;
    }

    public static double subtract(double left, double right)
    {
        return left - right;
    }

    public static double multiply(double left, double right)
    {
        return left * right;
    }

    public static double divide(double left, double right)
    {
        return left / right;
    }

    public static double modulus(double left, double right)
    {
        return left % right;
    }

    public static double negate(double value)
    {
        return -value;
    }

    public static boolean notEqual(long left, long right)
    {
        return left != right;
    }

    public static boolean lessThan(long left, long right)
    {
        return left < right;
    }

    public static boolean lessThanOrEqual(long left, long right)
    {
        return left <= right;
    }

    public static boolean greaterThan(long left, long right)
    {
        return left > right;
    }

    public static boolean greaterThanOrEqual(long left, long right)
    {
        return left >= right;
    }

    public static boolean equal(double left, double right)
    {
        return left == right;
    }

    public static boolean notEqual(double left, double right)
    {
        return left != right;
    }

    public static boolean lessThan(double left, double right)
    {
        return left < right;
    }

    public static boolean lessThanOrEqual(double left, double right)
    {
        return left <= right;
    }

    public static boolean greaterThan(double left, double right)
    {
        return left > right;
    }

    public static boolean greaterThanOrEqual(double left, double right)
    {
        return left >= right;
    }

    public static boolean equal(String left, String right)
    {
        return left.equals(right);
    }

    public static boolean notEqual(String left, String right)
    {
        return !left.equals(right);
    }

    public static boolean lessThan(String left, String right)
    {
        return left.compareTo(right) < 0;
    }

    public static boolean lessThanOrEqual(String left, String right)
    {
        return left.compareTo(right) <= 0;
    }

    public static boolean greaterThan(String left, String right)
    {
        return left.compareTo(right) > 0;
    }

    public static boolean greaterThanOrEqual(String left, String right)
    {
        return left.compareTo(right) >= 0;
    }

    public static String toString(Slice slice)
    {
        return slice.toString(Charsets.UTF_8);
    }

    public static boolean castToBoolean(boolean value)
    {
        return value;
    }

    public static boolean castToBoolean(long value)
    {
        return value != 0;
    }

    public static boolean castToBoolean(double value)
    {
        return value != 0;
    }

    public static boolean castToBoolean(String value)
    {
        if (value.equals("t") || value.equals("true") || value.equals("1")) {
            return true;
        }
        if (value.equals("f") || value.equals("false") || value.equals("0")) {
            return false;
        }
        throw new IllegalArgumentException(String.format("Cannot cast %s to BOOLEAN", value.getClass().getSimpleName()));
    }

    public static long castToLong(boolean value)
    {
        return value ? 1 : 0;
    }

    public static long castToLong(long value)
    {
        return value;
    }

    public static long castToLong(double value)
    {
        return (long) MathFunctions.round(value);
    }

    public static long castToLong(String value)
    {
        return Long.parseLong(value);
    }

    public static double castToDouble(boolean value)
    {
        return value ? 1 : 0;
    }

    public static double castToDouble(long value)
    {
        return value;
    }

    public static double castToDouble(double value)
    {
        return value;
    }

    public static double castToDouble(String value)
    {
        return Double.parseDouble(value);
    }

    public static String castToString(boolean value)
    {
        return value ? "true" : "false";
    }

    public static String castToString(long value)
    {
        return String.valueOf(value);
    }

    public static String castToString(double value)
    {
        return String.valueOf(value);
    }

    public static String castToString(String value)
    {
        return value;
    }
}
