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

import com.facebook.presto.client.StatementClient;
import com.google.common.collect.Lists;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class OutputHandler
        implements Closeable
{
    private static final int MAX_BUFFERED_ROWS = 1;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<List<?>> rowBuffer = new ArrayList<>(MAX_BUFFERED_ROWS);
    private final OutputPrinter printer;
    private long bufferStart;

    public OutputHandler(OutputPrinter printer)
    {
        this.printer = requireNonNull(printer, "printer is null");
    }

    public void processRow(List<?> row)
            throws IOException
    {
        if (rowBuffer.isEmpty()) {
            bufferStart = System.nanoTime();
        }

        rowBuffer.add(row);
        flush(false);
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            flush(true);
            printer.finish();
        }
    }

    public void processRows(StatementClient client, PrintStream out)
            throws IOException
    {
        int previousLine = 0;
        ConsolePrinter console = new ConsolePrinter(out);
        while (client.isRunning()) {
            Iterable<List<Object>> data = client.currentData().getData();
            if (data != null) {
                List<List<Object>> datalist = Lists.newArrayList(data);
                console.setLines(previousLine);
                console.resetScreen();
                for (List<Object> row : datalist) {
                    processRow(unmodifiableList(row));
                }
                previousLine = datalist.size();
            }
            client.advance();
        }
    }

    private void flush(boolean complete)
            throws IOException
    {
        if (!rowBuffer.isEmpty()) {
            printer.printRows(unmodifiableList(rowBuffer), complete);
            rowBuffer.clear();
        }
    }
}
