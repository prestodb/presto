package com.facebook.presto.operator.scalar;

import java.util.concurrent.ThreadLocalRandom;

public final class MathFunctions
{
    private MathFunctions() {}

    @ScalarFunction
    public static long abs(long num)
    {
        return Math.abs(num);
    }

    @ScalarFunction
    public static double abs(double num)
    {
        return Math.abs(num);
    }

    @ScalarFunction
    public static double acos(double num)
    {
        return Math.acos(num);
    }

    @ScalarFunction
    public static double asin(double num)
    {
        return Math.asin(num);
    }

    @ScalarFunction
    public static double atan(double num)
    {
        return Math.atan(num);
    }

    @ScalarFunction
    public static double atan2(double num1, double num2)
    {
        return Math.atan2(num1, num2);
    }

    @ScalarFunction
    public static double cbrt(double num)
    {
        return Math.cbrt(num);
    }

    @ScalarFunction(value = "ceil", alias = "ceiling")
    public static long ceiling(long num)
    {
        return num;
    }

    @ScalarFunction(value = "ceil", alias = "ceiling")
    public static double ceiling(double num)
    {
        return Math.ceil(num);
    }

    @ScalarFunction
    public static double cos(double num)
    {
        return Math.cos(num);
    }

    @ScalarFunction
    public static double cosh(double num)
    {
        return Math.cosh(num);
    }

    @ScalarFunction
    public static double e()
    {
        return Math.E;
    }

    @ScalarFunction
    public static double exp(double num)
    {
        return Math.exp(num);
    }

    @ScalarFunction
    public static long floor(long num)
    {
        return num;
    }

    @ScalarFunction
    public static double floor(double num)
    {
        return Math.floor(num);
    }

    @ScalarFunction
    public static double ln(double num)
    {
        return Math.log(num);
    }

    @ScalarFunction
    public static double log2(double num)
    {
        return Math.log(num) / Math.log(2);
    }

    @ScalarFunction
    public static double log10(double num)
    {
        return Math.log10(num);
    }

    @ScalarFunction
    public static double log(double num, double base)
    {
        return Math.log(num) / Math.log(base);
    }

    @ScalarFunction
    public static long mod(long num1, long num2)
    {
        return num1 % num2;
    }

    @ScalarFunction
    public static double mod(double num1, double num2)
    {
        return num1 % num2;
    }

    @ScalarFunction
    public static double pi()
    {
        return Math.PI;
    }

    @ScalarFunction
    public static double pow(double num, double exponent)
    {
        return Math.pow(num, exponent);
    }

    @ScalarFunction(alias = "rand", deterministic = false)
    public static double random()
    {
        return ThreadLocalRandom.current().nextDouble();
    }

    @ScalarFunction
    public static long round(long num)
    {
        return round(num, 0);
    }

    @ScalarFunction
    public static long round(long num, long decimals)
    {
        return num;
    }

    @ScalarFunction
    public static double round(double num)
    {
        return round(num, 0);
    }

    @ScalarFunction
    public static double round(double num, long decimals)
    {
        if (num == 0.0) {
            return 0;
        }
        if (num < 0) {
            return -round(-num, decimals);
        }

        double factor = Math.pow(10, decimals);
        return Math.floor(num * factor + 0.5) / factor;
    }

    @ScalarFunction
    public static double sin(double num)
    {
        return Math.sin(num);
    }

    @ScalarFunction
    public static double sqrt(double num)
    {
        return Math.sqrt(num);
    }

    @ScalarFunction
    public static double tan(double num)
    {
        return Math.tan(num);
    }

    @ScalarFunction
    public static double tanh(double num)
    {
        return Math.tanh(num);
    }
}
