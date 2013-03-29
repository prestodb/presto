package com.facebook.presto.cli;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static java.lang.Math.max;
import static java.lang.String.format;

public class AlignedTuplePrinter
        extends OutputHandler
{
    private static final int MAX_BUFFERED_ROWS = 10_000;
    private static final Splitter LINE_SPLITTER = Splitter.on('\n');

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final List<String> fieldNames;
    private final Writer writer;
    private boolean headerOutput;
    private long rowCount;

    public AlignedTuplePrinter(List<String> fieldNames, Writer writer)
    {
        this.fieldNames = ImmutableList.copyOf(checkNotNull(fieldNames, "fieldNames is null"));
        this.writer = checkNotNull(writer, "writer is null");
    }

    @Override
    public void processRow(List<?> values)
            throws IOException
    {
        checkState(fieldNames.size() == values.size(), "field names size does not match row size");
        rowBuffer.add(values);
        rowCount++;
        if (rowBuffer.size() == MAX_BUFFERED_ROWS) {
            flush();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            flush();
            writer.append(format("(%s row%s)%n", rowCount, (rowCount != 1) ? "s" : ""));
            writer.flush();
        }
    }

    private void flush()
            throws IOException
    {
        int columns = fieldNames.size();

        int[] maxWidth = new int[columns];
        for (int i = 0; i < columns; i++) {
            maxWidth[i] = max(1, fieldNames.get(i).length());
        }
        for (List<?> row : rowBuffer) {
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

        for (List<?> row : rowBuffer) {
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
                    String s = "";
                    if (columnLines.get(column).size() > line) {
                        s = columnLines.get(column).get(line);
                    }
                    boolean numeric = row.get(column) instanceof Number;
                    String out = align(s, maxWidth[column], 1, numeric);
                    if ((line + 1) < columnLines.get(column).size()) {
                        out = out.substring(0, out.length() - 1) + "+";
                    }
                    writer.append(out);
                }
                writer.append('\n');
            }
        }

        writer.flush();
        rowBuffer.clear();
    }

    private static String formatValue(Object o)
    {
        return (o == null) ? "NULL" : o.toString();
    }

    private static String center(String s, int maxWidth, int padding)
    {
        checkState(s.length() <= maxWidth, "string length is greater than max width");
        int left = (maxWidth - s.length()) / 2;
        int right = maxWidth - (left + s.length());
        return repeat(" ", left + padding) + s + repeat(" ", right + padding);
    }

    private static String align(String s, int maxWidth, int padding, boolean right)
    {
        checkState(s.length() <= maxWidth, "string length is greater than max width");
        String large = repeat(" ", (maxWidth - s.length()) + padding);
        String small = repeat(" ", padding);
        return right ? (large + s + small) : (small + s + large);
    }

    private static int maxLineLength(String s)
    {
        int n = 0;
        for (String line : LINE_SPLITTER.split(s)) {
            n = max(n, line.length());
        }
        return n;
    }
}
