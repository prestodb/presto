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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestAlignedTablePrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

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

    @Test
    public void testAlignedPrintingOneRow()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(row("a long line\nwithout wrapping", "text")), true);
        printer.finish();

        String expected = "" +
                "      first       | last \n" +
                "------------------+------\n" +
                " a long line      | text \n" +
                " without wrapping |      \n" +
                "(1 row)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.finish();

        String expected = "" +
                " first | last \n" +
                "-------+------\n" +
                "(0 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingHex()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "binary", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", bytes("hello"), "world"),
                row("a", bytes("some long text that is more than 16 bytes"), "b"),
                row("cat", bytes(""), "dog")),
                true);
        printer.finish();

        String expected = "" +
                " first |                     binary                      | last  \n" +
                "-------+-------------------------------------------------+-------\n" +
                " hello | 68 65 6c 6c 6f                                  | world \n" +
                " a     | 73 6f 6d 65 20 6c 6f 6e 67 20 74 65 78 74 20 74+| b     \n" +
                "       | 68 61 74 20 69 73 20 6d 6f 72 65 20 74 68 61 6e+|       \n" +
                "       | 20 31 36 20 62 79 74 65 73                      |       \n" +
                " cat   |                                                 | dog   \n" +
                "(3 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingWideCharacters()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("go\u7f51", "last", "quantity\u7f51");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "wide\u7f51", 123),
                row("some long\ntext \u7f51\ndoes not\u7f51\nfit", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = "" +
                "    go\u7f51    |  last  | quantity\u7f51 \n" +
                "------------+--------+------------\n" +
                " hello      | wide\u7f51 |        123 \n" +
                " some long +| more  +|       4567 \n" +
                " text \u7f51   +| text   |            \n" +
                " does not\u7f51+|        |            \n" +
                " fit        |        |            \n" +
                " bye        | done   |        -15 \n" +
                "(3 rows)\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    static List<?> row(Object... values)
    {
        return asList(values);
    }

    static List<List<?>> rows(List<?>... rows)
    {
        return asList(rows);
    }

    static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
