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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.TimeoutHandler;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import java.lang.ref.WeakReference;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class AsyncResponseUtils
{
    private AsyncResponseUtils()
    {
    }

    public static void registerAsyncResponse(
            AsyncResponse asyncResponse,
            ListenableFuture<?> futureResponse,
            Duration timeout,
            Executor executor)
    {
        Response timeoutResponse = Response.status(Status.SERVICE_UNAVAILABLE)
                .entity("Timed out after waiting for " + timeout.convertToMostSuccinctTimeUnit())
                .build();

        registerAsyncResponse(asyncResponse,
                futureResponse,
                timeout,
                executor,
                timeoutResponse);
    }

    public static void registerAsyncResponse(
            AsyncResponse asyncResponse,
            final ListenableFuture<?> futureResponse,
            Duration requestTimeout,
            Executor executor,
            final Response timeoutResponse)
    {
        // when the future completes, send the response
        Futures.addCallback(futureResponse, toAsyncResponse(asyncResponse), executor);

        // if the future does not complete in the specified time, send the timeout response
        asyncResponse.setTimeoutHandler(new AsyncTimeoutHandler(futureResponse, timeoutResponse));
        asyncResponse.setTimeout(requestTimeout.toMillis(), MILLISECONDS);
    }

    private static <T> FutureCallback<T> toAsyncResponse(final AsyncResponse asyncResponse)
    {
        return new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T value)
            {
                checkArgument(!(value instanceof ResponseBuilder), "Value is a ResponseBuilder. Did you forget to call build?");
                asyncResponse.resume(value);
            }

            @Override
            public void onFailure(Throwable t)
            {
                asyncResponse.resume(t);
            }
        };
    }

    private static class AsyncTimeoutHandler
            implements TimeoutHandler
    {
        private final WeakReference<ListenableFuture<?>> futureResponseReference;
        private final Response timeoutResponse;

        public AsyncTimeoutHandler(ListenableFuture<?> futureResponse, Response timeoutResponse)
        {
            // the jaxrs implementation can hold on to the async timeout for a long time, and
            // the future can reference large expensive objects.  Since we are only interested
            // in canceling this future on a timeout, only hold a weak reference to the future
            this.futureResponseReference = new WeakReference<ListenableFuture<?>>(futureResponse);
            this.timeoutResponse = timeoutResponse;
        }

        @Override
        public void handleTimeout(AsyncResponse asyncResponse)
        {
            asyncResponse.resume(timeoutResponse);

            // cancel the original future if it still exists
            ListenableFuture<?> futureResponse = futureResponseReference.get();
            if (futureResponse != null) {
                try {
                    futureResponse.cancel(true);
                }
                catch (Exception ignored) {
                }
            }
        }
    }
}
