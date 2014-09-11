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

import java.lang.ref.WeakReference;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.Response.status;

public class AsyncResponseHandler
{
    private final AsyncResponse asyncResponse;
    private final WeakReference<ListenableFuture<?>> futureResponseReference;

    private AsyncResponseHandler(AsyncResponse asyncResponse, ListenableFuture<?> futureResponse)
    {
        this.asyncResponse = checkNotNull(asyncResponse, "asyncResponse is null");
        // the jaxrs implementation can hold on to the async timeout for a long time, and
        // the future can reference large expensive objects.  Since we are only interested
        // in canceling this future on a timeout, only hold a weak reference to the future
        this.futureResponseReference = new WeakReference<ListenableFuture<?>>(checkNotNull(futureResponse, "futureResponse is null"));
    }

    public static AsyncResponseHandler bindAsyncResponse(AsyncResponse asyncResponse, ListenableFuture<?> futureResponse, Executor httpResponseExecutor)
    {
        Futures.addCallback(futureResponse, toFutureCallback(asyncResponse), httpResponseExecutor);
        return new AsyncResponseHandler(asyncResponse, futureResponse);
    }

    public AsyncResponseHandler withTimeout(Duration timeout)
    {
        return withTimeout(timeout,
                status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity("Timed out after waiting for " + timeout.convertToMostSuccinctTimeUnit())
                        .build());
    }

    public AsyncResponseHandler withTimeout(Duration timeout, final Response timeoutResponse)
    {
        asyncResponse.setTimeoutHandler(new TimeoutHandler()
        {
            @Override
            public void handleTimeout(AsyncResponse asyncResponse)
            {
                asyncResponse.resume(timeoutResponse);
                cancelFuture();
            }
        });
        asyncResponse.setTimeout(timeout.toMillis(), MILLISECONDS);
        return this;
    }

    private void cancelFuture()
    {
        // Cancel the original future if it still exists
        ListenableFuture<?> futureResponse = futureResponseReference.get();
        if (futureResponse != null) {
            try {
                futureResponse.cancel(true);
            }
            catch (Exception ignored) {
            }
        }
    }

    private static <T> FutureCallback<T> toFutureCallback(final AsyncResponse asyncResponse)
    {
        return new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T value)
            {
                checkArgument(!(value instanceof Response.ResponseBuilder), "Value is a ResponseBuilder. Did you forget to call build?");
                asyncResponse.resume(value);
            }

            @Override
            public void onFailure(Throwable t)
            {
                asyncResponse.resume(t);
            }
        };
    }
}
