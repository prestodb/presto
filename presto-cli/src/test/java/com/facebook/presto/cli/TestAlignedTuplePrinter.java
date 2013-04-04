package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestAlignedTuplePrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputHandler printer = new AlignedTuplePrinter(fieldNames, writer);

        printer.processRow(row("hello", "world", 123));
        printer.processRow(row("a", null, 4.5));
        printer.processRow(row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567));
        printer.processRow(row("bye", "done", -15));
        printer.close();

        String expected = "" +
                "   first   | last  | quantity \n" +
                "-----------+-------+----------\n" +
                " hello     | world |      123 \n" +
                " a         | NULL  |      4.5 \n" +
                " some long+| more +|     4567 \n" +
                " text that+| text  |          \n" +
                " does not +|       |          \n" +
                " fit on   +|       |          \n" +
                " one line  |       |          \n" +
                " bye       | done  |      -15 \n" +
                "(4 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    private static List<Object> row(Object... values)
    {
        return Arrays.asList(values);
    }
}
