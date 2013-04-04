package com.facebook.presto.cli;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CSVPrinter
        extends OutputHandler
{
    private final CSVWriter writer;

    public CSVPrinter(Writer writer, char separator)
    {
        checkNotNull(writer, "writer is null");
        this.writer = new CSVWriter(writer, separator);
    }

    @Override
    public void processRow(List<?> values)
            throws IOException
    {
        writer.writeNext(toStrings(values));
        checkError();
    }

    @Override
    public void close()
            throws IOException
    {
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
