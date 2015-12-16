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

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import static com.facebook.presto.cli.TestAlignedTablePrinter.row;
import static com.facebook.presto.cli.TestAlignedTablePrinter.rows;
import static org.testng.Assert.assertEquals;

public class TestTsvPrinter
{
    @Test
    public void testTsvPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new TsvPrinter(fieldNames, writer, true);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext\tdone", "more\ntext", 4567),
                row("bye", "done", -15),
                row("oops\0a\nb\rc\bd\fe\tf\\g\1done", "escape", 9)),
                true);
        printer.finish();

        String expected = "" +
                "first\tlast\tquantity\n" +
                "hello\tworld\t123\n" +
                "a\t\t4.5\n" +
                "some long\\ntext\\tdone\tmore\\ntext\t4567\n" +
                "bye\tdone\t-15\n" +
                "oops\\0a\\nb\\rc\\bd\\fe\\tf\\\\g\1done\tescape\t9\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testTsvPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new TsvPrinter(fieldNames, writer, true);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "first\tlast\n");
    }

    @Test
    public void testTsvPrintingNoHeader()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new TsvPrinter(fieldNames, writer, false);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5)),
                true);
        printer.finish();

        String expected = "" +
                "hello\tworld\t123\n" +
                "a\t\t4.5\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testCsvVarbinaryPrinting()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new TsvPrinter(fieldNames, writer, false);

        printer.printRows(rows(row("hello".getBytes(), null, 123)), true);
        printer.finish();

        String expected = "68 65 6c 6c 6f\t\t123\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }
}
