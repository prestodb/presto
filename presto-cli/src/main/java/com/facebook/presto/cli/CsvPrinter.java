package com.facebook.presto.cli;

import au.com.bytecode.opencsv.CSVWriter;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CsvPrinter
        implements OutputPrinter
{
    private final List<String> fieldNames;
    private final CSVWriter writer;

    private boolean needHeader;

    public CsvPrinter(List<String> fieldNames, Writer writer, boolean header)
    {
        checkNotNull(fieldNames, "fieldNames is null");
        checkNotNull(writer, "writer is null");
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.writer = new CSVWriter(writer);
        this.needHeader = header;
    }

    @Override
    public void printRows(List<List<?>> rows, boolean complete)
            throws IOException
    {
        if (needHeader) {
            needHeader = false;
            writer.writeNext(toStrings(fieldNames));
        }

        for (List<?> row : rows) {
            writer.writeNext(toStrings(row));
            checkError();
        }
    }

    @Override
    public void finish()
            throws IOException
    {
        printRows(ImmutableList.<List<?>>of(), true);
        writer.flush();
        checkError();
    }

    private void checkError()
            throws IOException
    {
        if (writer.checkError()) {
            throw new IOException("error writing to output");
        }
    }

    private static String[] toStrings(List<?> values)
    {
        String[] array = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            array[i] = (value == null) ? "" : value.toString();
        }
        return array;
    }
}
