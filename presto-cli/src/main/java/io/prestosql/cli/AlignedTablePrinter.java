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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.fusesource.jansi.AnsiString;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static jline.console.WCWidth.wcwidth;

public class AlignedTablePrinter
        implements OutputPrinter
{
    private static final Splitter LINE_SPLITTER = Splitter.on('\n');
    private static final Splitter HEX_SPLITTER = Splitter.fixedLength(2);
    private static final Joiner HEX_BYTE_JOINER = Joiner.on(' ');
    private static final Joiner HEX_LINE_JOINER = Joiner.on('\n');

    private final List<String> fieldNames;
    private final Writer writer;

    private boolean headerOutput;
    private long rowCount;

    public AlignedTablePrinter(List<String> fieldNames, Writer writer)
    {
        this.fieldNames = ImmutableList.copyOf(requireNonNull(fieldNames, "fieldNames is null"));
        this.writer = requireNonNull(writer, "writer is null");
    }

    @Override
    public void finish()
            throws IOException
    {
        printRows(ImmutableList.of(), true);
        writer.append(format("(%s row%s)%n", rowCount, (rowCount != 1) ? "s" : ""));
        writer.flush();
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        rowCount += rows.size();
        int columns = fieldNames.size();

        int[] maxWidth = new int[columns];
        for (int i = 0; i < columns; i++) {
            maxWidth[i] = max(1, consoleWidth(fieldNames.get(i)));
        }
        for (List<?> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String s = formatValue(row.get(i));
                maxWidth[i] = max(maxWidth[i], maxLineLength(s));
            }
        }

        if (!headerOutput) {
            headerOutput = true;

            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('|');
                }
                String name = fieldNames.get(i);
                writer.append(center(name, maxWidth[i], 1));
            }
            writer.append('\n');

            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('+');
                }
                writer.append(repeat("-", maxWidth[i] + 2));
            }
            writer.append('\n');
        }

        for (List<?> row : rows) {
            List<List<String>> columnLines = new ArrayList<>(columns);
            int maxLines = 1;
            for (int i = 0; i < columns; i++) {
                String s = formatValue(row.get(i));
                ImmutableList<String> lines = ImmutableList.copyOf(LINE_SPLITTER.split(s));
                columnLines.add(lines);
                maxLines = max(maxLines, lines.size());
            }

            for (int line = 0; line < maxLines; line++) {
                for (int column = 0; column < columns; column++) {
                    if (column > 0) {
                        writer.append('|');
                    }
                    List<String> lines = columnLines.get(column);
                    String s = (line < lines.size()) ? lines.get(line) : "";
                    boolean numeric = row.get(column) instanceof Number;
                    String out = align(s, maxWidth[column], 1, numeric);
                    if ((!complete || (rowCount > 1)) && ((line + 1) < lines.size())) {
                        out = out.substring(0, out.length() - 1) + "+";
                    }
                    writer.append(out);
                }
                writer.append('\n');
            }
        }

        writer.flush();
    }

    static String formatValue(Object o)
    {
        if (o == null) {
            return "NULL";
        }

        if (o instanceof byte[]) {
            return formatHexDump((byte[]) o, 16);
        }

        return o.toString();
    }

    private static String formatHexDump(byte[] bytes, int bytesPerLine)
    {
        // hex pairs: ["61", "62", "63"]
        Iterable<String> hexPairs = createHexPairs(bytes);

        // hex lines: [["61", "62", "63], [...]]
        Iterable<List<String>> hexLines = partition(hexPairs, bytesPerLine);

        // lines: ["61 62 63", ...]
        Iterable<String> lines = transform(hexLines, HEX_BYTE_JOINER::join);

        // joined: "61 62 63\n..."
        return HEX_LINE_JOINER.join(lines);
    }

    static String formatHexDump(byte[] bytes)
    {
        return HEX_BYTE_JOINER.join(createHexPairs(bytes));
    }

    private static Iterable<String> createHexPairs(byte[] bytes)
    {
        // hex dump: "616263"
        String hexDump = base16().lowerCase().encode(bytes);

        // hex pairs: ["61", "62", "63"]
        return HEX_SPLITTER.split(hexDump);
    }

    private static String center(String s, int maxWidth, int padding)
    {
        int width = consoleWidth(s);
        checkState(width <= maxWidth, "string width is greater than max width");
        int left = (maxWidth - width) / 2;
        int right = maxWidth - (left + width);
        return repeat(" ", left + padding) + s + repeat(" ", right + padding);
    }

    private static String align(String s, int maxWidth, int padding, boolean right)
    {
        int width = consoleWidth(s);
        checkState(width <= maxWidth, "string width is greater than max width");
        String large = repeat(" ", (maxWidth - width) + padding);
        String small = repeat(" ", padding);
        return right ? (large + s + small) : (small + s + large);
    }

    static int maxLineLength(String s)
    {
        int n = 0;
        for (String line : LINE_SPLITTER.split(s)) {
            n = max(n, consoleWidth(line));
        }
        return n;
    }

    static int consoleWidth(String s)
    {
        return consoleWidth(new AnsiString(s));
    }

    private static int consoleWidth(AnsiString s)
    {
        CharSequence plain = s.getPlain();
        int n = 0;
        for (int i = 0; i < plain.length(); i++) {
            n += max(wcwidth(plain.charAt(i)), 0);
        }
        return n;
    }
}
