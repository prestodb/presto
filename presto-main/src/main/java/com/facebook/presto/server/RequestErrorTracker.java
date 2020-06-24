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
package com.facebook.presto.server;

import com.facebook.airlift.event.client.ServiceUnavailableException;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.server.remotetask.Backoff;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
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

import static com.facebook.presto.spi.HostAddress.fromUri;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class RequestErrorTracker
{
    private static final Logger log = Logger.get(RequestErrorTracker.class);

    private final Object id;
    private final URI uri;
    private ErrorCodeSupplier errorCode;
    private String nodeErrorMessage;
    private final ScheduledExecutorService scheduledExecutor;
    private final String jobDescription;
    private final Backoff backoff;

    private final Queue<Throwable> errorsSinceLastSuccess = new ConcurrentLinkedQueue<>();

    private RequestErrorTracker(Object id, URI uri, ErrorCodeSupplier errorCode, String nodeErrorMessage, Duration maxErrorDuration, ScheduledExecutorService scheduledExecutor, String jobDescription)
    {
        this.id = requireNonNull(id, "id is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
        this.nodeErrorMessage = requireNonNull(nodeErrorMessage, "nodeErrorMessage is null");
        this.scheduledExecutor = requireNonNull(scheduledExecutor, "scheduledExecutor is null");
        this.backoff = new Backoff(requireNonNull(maxErrorDuration, "maxErrorDuration is null"));
        this.jobDescription = requireNonNull(jobDescription, "jobDescription is null");
    }

    public static RequestErrorTracker taskRequestErrorTracker(TaskId taskId, URI taskUri, Duration maxErrorDuration, ScheduledExecutorService scheduledExecutor, String jobDescription)
    {
        return new RequestErrorTracker(taskId, taskUri, REMOTE_TASK_ERROR, WORKER_NODE_ERROR, maxErrorDuration, scheduledExecutor, jobDescription);
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
        backoff.startRequest();
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
            throw new PrestoException(errorCode, reason);
        }

        // log failure message
        if (isExpectedError(reason)) {
            // don't print a stack for a known errors
            log.warn("Error " + jobDescription + " %s: %s: %s", id, reason.getMessage(), uri);
        }
        else {
            log.warn(reason, "Error " + jobDescription + " %s: %s", id, uri);
        }

        // remember the first 10 errors
        if (errorsSinceLastSuccess.size() < 10) {
            errorsSinceLastSuccess.add(reason);
        }

        // fail the operation, if we have more than X failures in a row and more than Y seconds have passed since the last request
        if (backoff.failure()) {
            // it is weird to mark the task failed locally and then cancel the remote task, but there is no way to tell a remote task that it is failed
            PrestoException exception = new PrestoTransportException(TOO_MANY_REQUESTS_FAILED,
                    fromUri(uri),
                    format("%s (%s %s - %s failures, failure duration %s, total failed request time %s)",
                            nodeErrorMessage,
                            jobDescription,
                            uri,
                            backoff.getFailureCount(),
                            backoff.getFailureDuration().convertTo(SECONDS),
                            backoff.getFailureRequestTimeTotal().convertTo(SECONDS)));
            errorsSinceLastSuccess.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    public static boolean isExpectedError(Throwable t)
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
