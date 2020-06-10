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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.operator.ExchangeClientConfig;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.server.SerializedPageWriteListener.PAGE_METADATA_SIZE;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static com.facebook.presto.util.TaskUtils.DEFAULT_MAX_WAIT_TIME;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.Futures.addCallback;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NO_CONTENT;

@RolesAllowed(INTERNAL)
public class AsyncPageTransportServlet
        extends HttpServlet
{
    private static final Logger log = Logger.get(AsyncPageTransportServlet.class);

    private final Duration pageTransportTimeout;
    private final TaskManager taskManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public AsyncPageTransportServlet(
            TaskManager taskManager,
            ExchangeClientConfig exchangeClientConfig,
            @ForAsyncRpc BoundedExecutor responseExecutor,
            @ForAsyncRpc ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.pageTransportTimeout = requireNonNull(exchangeClientConfig.getAsyncPageTransportTimeout(), "asyncPageTransportTimeout is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException
    {
        String requestURI = request.getRequestURI();
        // Example:  /v1/task/async/{taskId}/results/{bufferId}/{token}
        List<String> requestURIParts = Arrays.asList(requestURI.split("/"));

        if (requestURIParts.size() != 8) {
            response.sendError(SC_BAD_REQUEST, format("Unexpected URI for task result request in async mode: %s", requestURI));
            return;
        }

        TaskId taskId = TaskId.valueOf(requestURIParts.get(4));
        OutputBufferId bufferId = OutputBufferId.fromString(requestURIParts.get(6));
        long token = parseLong(requestURIParts.get(7));
        DataSize maxSize = DataSize.valueOf(request.getHeader(PRESTO_MAX_SIZE));

        AsyncContext asyncContext = request.startAsync(request, response);

        // wait time to get results
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        asyncContext.setTimeout(waitTime.toMillis() + pageTransportTimeout.toMillis());

        asyncContext.addListener(new AsyncListener()
        {
            public void onComplete(AsyncEvent event)
            {
            }

            public void onError(AsyncEvent event)
                    throws IOException
            {
                String errorMessage = format("Server error to process task result request %s : %s", requestURI, event.getThrowable().getMessage());
                log.error(event.getThrowable(), errorMessage);
                response.sendError(SC_INTERNAL_SERVER_ERROR, errorMessage);
            }

            public void onStartAsync(AsyncEvent event)
            {
            }

            public void onTimeout(AsyncEvent event)
                    throws IOException
            {
                String errorMessage = format("Server timeout to process task result request: %s", requestURI);
                log.error(event.getThrowable(), errorMessage);
                response.sendError(SC_INTERNAL_SERVER_ERROR, errorMessage);
            }
        });

        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, maxSize);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);

        ServletOutputStream out = response.getOutputStream();
        addCallback(bufferResultFuture, new FutureCallback<BufferResult>()
                {
                    @Override
                    public void onSuccess(BufferResult bufferResult)
                    {
                        List<SerializedPage> serializedPages = new LinkedList<>(bufferResult.getSerializedPages());

                        response.setHeader(CONTENT_TYPE, PRESTO_PAGES);
                        response.setHeader(PRESTO_TASK_INSTANCE_ID, bufferResult.getTaskInstanceId());
                        response.setHeader(PRESTO_PAGE_TOKEN, String.valueOf(bufferResult.getToken()));
                        response.setHeader(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(bufferResult.getNextToken()));
                        response.setHeader(PRESTO_BUFFER_COMPLETE, String.valueOf(bufferResult.isBufferComplete()));

                        if (serializedPages.isEmpty()) {
                            response.setStatus(SC_NO_CONTENT);
                            asyncContext.complete();
                        }
                        else {
                            int contentLength = serializedPages.stream()
                                    .mapToInt(page -> page.getSizeInBytes() + PAGE_METADATA_SIZE)
                                    .sum();
                            response.setHeader(CONTENT_LENGTH, String.valueOf(contentLength));
                            out.setWriteListener(new SerializedPageWriteListener(serializedPages, asyncContext, out));
                        }
                    }

                    @Override
                    public void onFailure(Throwable thrown)
                    {
                        String errorMessage = format("Error getting task result from TaskManager for request %s : %s", requestURI, thrown.getMessage());
                        log.error(thrown, errorMessage);
                        try {
                            response.sendError(SC_INTERNAL_SERVER_ERROR, errorMessage);
                        }
                        catch (IOException e) {
                            log.error(e, "Failed to send response with error code: %s", e.getMessage());
                        }
                        asyncContext.complete();
                    }
                },
                responseExecutor);
    }

    @Override
    public void destroy()
    {
    }
}
