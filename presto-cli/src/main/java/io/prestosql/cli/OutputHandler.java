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

import io.airlift.units.Duration;
import io.prestosql.client.StatementClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
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
        this.printer = requireNonNull(printer, "printer is null");
    }

    public void processRow(List<?> row)
            throws IOException
    {
        if (rowBuffer.isEmpty()) {
            bufferStart = System.nanoTime();
        }

        rowBuffer.add(row);
        if (rowBuffer.size() >= MAX_BUFFERED_ROWS) {
            flush(false);
        }
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

    public void processRows(StatementClient client)
            throws IOException
    {
        while (client.isRunning()) {
            Iterable<List<Object>> data = client.currentData().getData();
            if (data != null) {
                for (List<Object> row : data) {
                    processRow(unmodifiableList(row));
                }
            }

            if (nanosSince(bufferStart).compareTo(MAX_BUFFER_TIME) >= 0) {
                flush(false);
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
