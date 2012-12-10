package com.facebook.presto.cli;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static java.lang.Math.max;
import static java.lang.String.format;

public class AlignedTuplePrinter
        extends OutputHandler
{
    private static final int MAX_BUFFERED_ROWS = 10_000;
    private final List<List<Object>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final List<String> fieldNames;
    private final Writer writer;
    private boolean headerOutput = false;
    private long rowCount = 0;

    public AlignedTuplePrinter(List<String> fieldNames)
    {
        this(fieldNames, new OutputStreamWriter(System.out, Charsets.UTF_8));
    }

    public AlignedTuplePrinter(List<String> fieldNames, Writer writer)
    {
        this.fieldNames = ImmutableList.copyOf(checkNotNull(fieldNames, "fieldNames is null"));
        this.writer = checkNotNull(writer, "writer is null");
    }

    @Override
    public void process(List<Object> values)
    {
        checkState(fieldNames.size() == values.size(), "field names size does not match row size");
        rowBuffer.add(values);
        rowCount++;
        if (rowBuffer.size() == MAX_BUFFERED_ROWS) {
            flush();
        }
    }

    @Override
    public void finish()
    {
        flush();
        try {
            writer.append(format("(%s row%s)%n", rowCount, (rowCount != 1) ? "s" : ""));
            writer.flush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void flush()
    {
        try {
            doFlush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void doFlush()
            throws IOException
    {
        int columns = fieldNames.size();

        int maxWidth[] = new int[columns];
        for (int i = 0; i < columns; i++) {
            maxWidth[i] = max(1, fieldNames.get(i).length());
        }
        for (List<Object> row : rowBuffer) {
            for (int i = 0; i < row.size(); i++) {
                String s = formatValue(row.get(i));
                maxWidth[i] = max(maxWidth[i], s.length());
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

        for (List<Object> row : rowBuffer) {
            for (int i = 0; i < columns; i++) {
                if (i > 0) {
                    writer.append('|');
                }
                String s = formatValue(row.get(i));
                boolean numeric = row.get(i) instanceof Number;
                writer.append(align(s, maxWidth[i], 1, numeric));
            }
            writer.append('\n');
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
}
