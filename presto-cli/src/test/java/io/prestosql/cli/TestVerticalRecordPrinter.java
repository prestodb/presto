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
package io.prestosql.cli;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.util.List;

import static io.prestosql.cli.TestAlignedTablePrinter.bytes;
import static io.prestosql.cli.TestAlignedTablePrinter.row;
import static io.prestosql.cli.TestAlignedTablePrinter.rows;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("Duplicates")
public class TestVerticalRecordPrinter
{
    @Test
    public void testVerticalPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

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
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

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
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

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
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]--+------\n" +
                "order_priority | hello\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalWideCharacterName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("order_priority\u7f51");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]----+------\n" +
                "order_priority\u7f51 | hello\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalWideCharacterValue()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("name");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello\u7f51 bye")), true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]-----\n" +
                "name | hello\u7f51 bye\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("none");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "(no rows)\n");
    }

    @Test
    public void testVerticalPrintingHex()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "binary", "last");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", bytes("hello"), "world"),
                row("a", bytes("some long text that is more than 16 bytes"), "b"),
                row("cat", bytes(""), "dog")),
                true);
        printer.finish();

        String expected = "" +
                "-[ RECORD 1 ]-------------------------------------------\n" +
                "first  | hello\n" +
                "binary | 68 65 6c 6c 6f\n" +
                "last   | world\n" +
                "-[ RECORD 2 ]-------------------------------------------\n" +
                "first  | a\n" +
                "binary | 73 6f 6d 65 20 6c 6f 6e 67 20 74 65 78 74 20 74\n" +
                "       | 68 61 74 20 69 73 20 6d 6f 72 65 20 74 68 61 6e\n" +
                "       | 20 31 36 20 62 79 74 65 73\n" +
                "last   | b\n" +
                "-[ RECORD 3 ]-------------------------------------------\n" +
                "first  | cat\n" +
                "binary | \n" +
                "last   | dog\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }
}
