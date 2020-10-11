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
package com.facebook.presto.server.thrift;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.thrift.ThriftResponse;
import com.facebook.presto.server.ServiceUnavailableException;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandlerStats;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.FutureCallback;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftHttpResponseHandler<T>
        implements FutureCallback<ThriftResponse<T>>
{
    private final SimpleHttpResponseCallback<T> callback;
    private final URI uri;
    private final SimpleHttpResponseHandlerStats stats;
    private final ErrorCodeSupplier errorCode;

    public ThriftHttpResponseHandler(
            SimpleHttpResponseCallback<T> callback,
            URI uri,
            SimpleHttpResponseHandlerStats stats,
            ErrorCodeSupplier errorCode)
    {
        this.callback = callback;
        this.uri = uri;
        this.stats = requireNonNull(stats, "stats is null");
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
    }
    @Override
    public void onSuccess(ThriftResponse<T> response)
    {
        stats.updateSuccess();
        //TODO : Find a way to add response size from thrift
        try {
            if (response.getStatusCode() == OK.code() && response.getValue() != null) {
                callback.success(response.getValue());
            }
            else if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                callback.failed(new ServiceUnavailableException(uri));
            }
            else {
                // Something is broken in the server or the client, so fail immediately (includes 500 errors)
                Exception cause = response.getException();
                if (cause == null) {
                    if (response.getStatusCode() == OK.code()) {
                        cause = new PrestoException(errorCode, format("Expected response from %s is empty", uri));
                    }
                    else {
                        cause = new PrestoException(errorCode, createErrorMessage(response));
                    }
                }
                else {
                    cause = new PrestoException(errorCode, format("Unexpected response from %s", uri), cause);
                }
                callback.fatal(cause);
            }
        }
        catch (Throwable t) {
            callback.fatal(t);
        }
    }

    private String createErrorMessage(ThriftResponse<T> response)
    {
        return format("Expected response code from %s to be %s, but was %s: %s%n%s",
                uri,
                OK.code(),
                response.getStatusCode(),
                response.getStatusMessage(),
                response.getValue());
    }

    @Override
    public void onFailure(Throwable t)
    {
        stats.updateFailure();
        callback.failed(t);
    }
}
