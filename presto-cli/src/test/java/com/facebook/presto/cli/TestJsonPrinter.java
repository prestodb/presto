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

public class TestJsonPrinter
{
    @Test
    public void testJsonPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new JsonPrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext\tdone", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = "" +
                "{\"first\":\"hello\",\"last\":\"world\",\"quantity\":123}\n" +
                "{\"first\":\"a\",\"last\":null,\"quantity\":4.5}\n" +
                "{\"first\":\"some long\\ntext\\tdone\",\"last\":\"more\\ntext\",\"quantity\":4567}\n" +
                "{\"first\":\"bye\",\"last\":\"done\",\"quantity\":-15}\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testJsonPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new JsonPrinter(fieldNames, writer);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "");
    }

    @Test
    public void testJsonVarbinaryPrinting()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new JsonPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello".getBytes(), null, 123)), true);
        printer.finish();

        String expected = "{\"first\":\"68 65 6c 6c 6f\",\"last\":null,\"quantity\":123}\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }
}
