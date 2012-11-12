package com.facebook.presto.cli;

import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.Operator;
import io.airlift.units.Duration;

public class Utils
{
    public static void printResults(long start, Operator aggregation)
    {
        long rows = ConsolePrinter.print(aggregation);
        Duration duration = Duration.nanosSince(start);
        System.out.printf("%d rows in %4.2f ms\n", rows, duration.toMillis());
    }
}
