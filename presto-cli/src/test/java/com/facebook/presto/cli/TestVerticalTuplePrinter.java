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
package com.facebook.presto.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestVerticalTuplePrinter
{
    @Test
    public void testVerticalPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new VerticalTuplePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]-------\n" +
                "first    | hello\n" +
                "last     | world\n" +
                "quantity | 123\n" +
                "-[ RECORD 2 ]-------\n" +
                "first    | a\n" +
                "last     | NULL\n" +
                "quantity | 4.5\n" +
                "-[ RECORD 3 ]-------\n" +
                "first    | some long\n" +
                "         | text that\n" +
                "         | does not\n" +
                "         | fit on\n" +
                "         | one line\n" +
                "last     | more\n" +
                "         | text\n" +
                "quantity | 4567\n" +
                "-[ RECORD 4 ]-------\n" +
                "first    | bye\n" +
                "last     | done\n" +
                "quantity | -15\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalShortName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("a");
        OutputPrinter printer = new VerticalTuplePrinter(fieldNames, writer);

        printer.printRows(rows(row("x")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]\n" +
                "a | x\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalLongName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("shippriority");
        OutputPrinter printer = new VerticalTuplePrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]+------\n" +
                "shippriority | hello\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalLongerName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("order_priority");
        OutputPrinter printer = new VerticalTuplePrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]--+------\n" +
                "order_priority | hello\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("none");
        OutputPrinter printer = new VerticalTuplePrinter(fieldNames, writer);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "(no rows)\n");
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
