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
package com.facebook.presto.spark.execution;

import com.facebook.presto.execution.buffer.OutputBufferMemoryManager;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

public class PrestoSparkRowBuffer
{
    private final OutputBufferMemoryManager memoryManager;

    private final Object monitor = new Object();
    @GuardedBy("monitor")
    private final Queue<PrestoSparkRowBatch> buffer = new ArrayDeque<>();
    @GuardedBy("monitor")
    private boolean finished;

    public PrestoSparkRowBuffer(OutputBufferMemoryManager memoryManager)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
    }

    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    public void enqueue(PrestoSparkRowBatch rows)
    {
        requireNonNull(rows, "rows is null");
        synchronized (monitor) {
            buffer.add(rows);
            memoryManager.updateMemoryUsage(rows.getRetainedSizeInBytes());
            monitor.notify();
        }
    }

    public void setNoMoreRows()
    {
        memoryManager.setNoBlockOnFull();
        synchronized (monitor) {
            finished = true;
            monitor.notifyAll();
        }
    }

    public PrestoSparkRowBatch get()
            throws InterruptedException
    {
        PrestoSparkRowBatch rowBatch = null;
        synchronized (monitor) {
            while (buffer.isEmpty() && !finished) {
                monitor.wait();
            }
            if (!buffer.isEmpty()) {
                rowBatch = buffer.poll();
            }
            if (rowBatch != null) {
                memoryManager.updateMemoryUsage(-rowBatch.getRetainedSizeInBytes());
            }
        }
        return rowBatch;
    }
}
