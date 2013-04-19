package com.facebook.presto.cli;

import com.facebook.presto.client.StatementClient;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OutputHandler
        implements Closeable
{
    private static final Duration MAX_BUFFER_TIME = new Duration(3, SECONDS);
    private static final int MAX_BUFFERED_ROWS = 10_000;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final OutputPrinter printer;

    private long bufferStart;

    public OutputHandler(OutputPrinter printer)
    {
        this.printer = checkNotNull(printer, "printer is null");
    }

    public void processRow(List<?> row)
            throws IOException
    {
        if (rowBuffer.isEmpty()) {
            bufferStart = System.nanoTime();
        }

        rowBuffer.add(row);
        if (rowBuffer.size() >= MAX_BUFFERED_ROWS) {
            flush();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            flush();
            printer.finish();
        }
    }

    public void processRows(StatementClient client)
            throws IOException
    {
        while (client.isValid()) {
            Iterable<List<Object>> data = client.current().getData();
            if (data != null) {
                for (List<Object> tuple : data) {
                    processRow(unmodifiableList(tuple));
                }
            }

            if (nanosSince(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
                flush();
            }

            client.advance();
        }
    }

    private void flush()
            throws IOException
    {
        if (!rowBuffer.isEmpty()) {
            printer.printRows(unmodifiableList(rowBuffer));
            rowBuffer.clear();
        }
    }
}
