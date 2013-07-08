package com.facebook.presto.cli;

import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestCsvPrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        OutputPrinter printer = new CsvPrinter(writer, ',');

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

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

    private static List<?> row(Object... values)
    {
        return asList(values);
    }

    private static List<List<?>> rows(List<?>... rows)
    {
        return asList(rows);
    }
}
