package com.facebook.presto.cli;

import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestCsvPrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputHandler printer = new CsvPrinter(writer, ',');

        printer.processRow(row("hello", "world", 123));
        printer.processRow(row("a", null, 4.5));
        printer.processRow(row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567));
        printer.processRow(row("bye", "done", -15));
        printer.close();

        String expected = "" +
                "\"hello\",\"world\",\"123\"\n" +
                "\"a\",\"\",\"4.5\"\n" +
                "\"some long\n" +
                "text that\n" +
                "does not\n" +
                "fit on\n" +
                "one line\",\"more\n" +
                "text\",\"4567\"\n" +
                "\"bye\",\"done\",\"-15\"\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    private static List<Object> row(Object... values)
    {
        return Arrays.asList(values);
    }
}
