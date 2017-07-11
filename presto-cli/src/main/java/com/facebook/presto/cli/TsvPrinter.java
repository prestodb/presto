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

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.cli.CsvPrinter.formatValue;
import static java.util.Objects.requireNonNull;

public class TsvPrinter
        implements OutputPrinter
{
    private final List<String> fieldNames;
    private final Writer writer;

    private boolean needHeader;

    public TsvPrinter(List<String> fieldNames, Writer writer, boolean header)
    {
        this.fieldNames = ImmutableList.copyOf(requireNonNull(fieldNames, "fieldNames is null"));
        this.writer = requireNonNull(writer, "writer is null");
        this.needHeader = header;
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        if (needHeader) {
            needHeader = false;
            printRows(ImmutableList.of(fieldNames), false);
        }

        for (List<?> row : rows) {
            writer.write(formatRow(row));
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        printRows(ImmutableList.of(), true);
        writer.flush();
    }

    private static String formatRow(List<?> row)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<?> iter = row.iterator();
        while (iter.hasNext()) {
            String s = formatValue(iter.next());

            for (int i = 0; i < s.length(); i++) {
                escapeCharacter(sb, s.charAt(i));
            }

            if (iter.hasNext()) {
                sb.append('\t');
            }
        }
        sb.append('\n');
        return sb.toString();
    }

    private static void escapeCharacter(StringBuilder sb, char c)
    {
        switch (c) {
            case '\0':
                sb.append('\\').append('0');
                break;
            case '\b':
                sb.append('\\').append('b');
                break;
            case '\f':
                sb.append('\\').append('f');
                break;
            case '\n':
                sb.append('\\').append('n');
                break;
            case '\r':
                sb.append('\\').append('r');
                break;
            case '\t':
                sb.append('\\').append('t');
                break;
            case '\\':
                sb.append('\\').append('\\');
                break;
            default:
                sb.append(c);
        }
    }
}
