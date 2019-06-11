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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class OutputHandler
        implements Closeable
{
    private static final int MAX_QUEUED_ROWS = 50_000;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlockingQueue<List<?>> rowQueue = new LinkedBlockingQueue<>(MAX_QUEUED_ROWS);
    private final OutputPrinter printer;

    private CompletableFuture<Void> future;

    public OutputHandler(OutputPrinter printer)
    {
        this.printer = requireNonNull(printer, "printer is null");
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.getAndSet(true)) {
            printer.finish();
        }
    }

    public void processRows(StatementClient client)
            throws IOException
    {
        this.future = CompletableFuture.runAsync(() -> {
            while (client.isRunning()) {
                Iterable<List<Object>> data = client.currentData().getData();
                if (data != null) {
                    for (List<Object> row : data) {
                        try {
                            rowQueue.put(unmodifiableList(row));
                        }
                        catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
                client.advance();
            }
        });
        while (!future.isDone()) {
            while (!rowQueue.isEmpty()) {
                printer.printRow(rowQueue.poll(), false);
            }
        }
        while (!rowQueue.isEmpty()) {
            printer.printRow(rowQueue.poll(), false);
        }
        if (future.isCompletedExceptionally()) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
