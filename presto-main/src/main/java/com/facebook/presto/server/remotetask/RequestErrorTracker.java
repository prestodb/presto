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
package com.facebook.presto.server.remotetask;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ObjectArrays;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.airlift.event.client.ServiceUnavailableException;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
class RequestErrorTracker
{
    private static final Logger log = Logger.get(RequestErrorTracker.class);

    private final TaskId taskId;
    private final URI taskUri;
    private final ScheduledExecutorService scheduledExecutor;
    private final String jobDescription;
    private final Backoff backoff;

    private final Queue<Throwable> errorsSinceLastSuccess = new ConcurrentLinkedQueue<>();

    public RequestErrorTracker(TaskId taskId, URI taskUri, Duration minErrorDuration, ScheduledExecutorService scheduledExecutor, String jobDescription)
    {
        this.taskId = taskId;
        this.taskUri = taskUri;
        this.scheduledExecutor = scheduledExecutor;
        this.backoff = new Backoff(minErrorDuration);
        this.jobDescription = jobDescription;
    }

    public ListenableFuture<?> acquireRequestPermit()
    {
        long delayNanos = backoff.getBackoffDelayNanos();

        if (delayNanos == 0) {
            return Futures.immediateFuture(null);
        }

        ListenableFutureTask<Object> futureTask = ListenableFutureTask.create(() -> null);
        scheduledExecutor.schedule(futureTask, delayNanos, NANOSECONDS);
        return futureTask;
    }

    public void startRequest()
    {
        // before scheduling a new request clear the error timer
        // we consider a request to be "new" if there are no current failures
        if (backoff.getFailureCount() == 0) {
            requestSucceeded();
        }
    }

    public void requestSucceeded()
    {
        backoff.success();
        errorsSinceLastSuccess.clear();
    }

    public void requestFailed(Throwable reason)
            throws PrestoException
    {
        // cancellation is not a failure
        if (reason instanceof CancellationException) {
            return;
        }

        if (reason instanceof RejectedExecutionException) {
            throw new PrestoException(REMOTE_TASK_ERROR, reason);
        }

        // log failure message
        if (isExpectedError(reason)) {
            // don't print a stack for a known errors
            log.warn("Error " + jobDescription + " %s: %s: %s", taskId, reason.getMessage(), taskUri);
        }
        else {
            log.warn(reason, "Error " + jobDescription + " %s: %s", taskId, taskUri);
        }

        // remember the first 10 errors
        if (errorsSinceLastSuccess.size() < 10) {
            errorsSinceLastSuccess.add(reason);
        }

        // fail the task, if we have more than X failures in a row and more than Y seconds have passed since the last request
        if (backoff.failure()) {
            // it is weird to mark the task failed locally and then cancel the remote task, but there is no way to tell a remote task that it is failed
            PrestoException exception = new PrestoException(TOO_MANY_REQUESTS_FAILED,
                    format("%s (%s %s - %s failures, time since last success %s)",
                            WORKER_NODE_ERROR,
                            jobDescription,
                            taskUri,
                            backoff.getFailureCount(),
                            backoff.getTimeSinceLastSuccess().convertTo(SECONDS)));
            errorsSinceLastSuccess.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    static void logError(Throwable t, String format, Object... args)
    {
        if (isExpectedError(t)) {
            log.error(format + ": %s", ObjectArrays.concat(args, t));
        }
        else {
            log.error(t, format, args);
        }
    }

    private static boolean isExpectedError(Throwable t)
    {
        while (t != null) {
            if ((t instanceof SocketException) ||
                    (t instanceof SocketTimeoutException) ||
                    (t instanceof EOFException) ||
                    (t instanceof TimeoutException) ||
                    (t instanceof ServiceUnavailableException)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
