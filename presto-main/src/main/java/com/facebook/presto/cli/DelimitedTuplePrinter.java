package com.facebook.presto.cli;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static com.facebook.presto.operator.OutputProcessor.OutputHandler;
import static com.google.common.base.Preconditions.checkNotNull;

public class DelimitedTuplePrinter
        extends OutputHandler
{
    private final Writer writer;
    private final String delimiter;

    public DelimitedTuplePrinter(Writer writer, String delimiter)
    {
        this.writer = checkNotNull(writer, "writer is null");
        this.delimiter = checkNotNull(delimiter, "delimiter is null");
    }

    @Override
    public void processRow(List<?> values)
    {
        try {
            Joiner.on(delimiter)
                    .useForNull("NULL")
                    .appendTo(writer, values);
            writer.write('\n');
            writer.flush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
