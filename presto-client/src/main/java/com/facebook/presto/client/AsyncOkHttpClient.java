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
package com.facebook.presto.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AsyncOkHttpClient
{
    private final OkHttpClient httpClient;

    public AsyncOkHttpClient(OkHttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public Response execute(Request request)
            throws IOException
    {
        return httpClient.newCall(request).execute();
    }

    public ListenableFuture<Response> executeAsync(Request request)
    {
        SettableFuture<Response> future = SettableFuture.create();
        httpClient.newCall(request).enqueue(new OkHttpCallback(future));
        return future;
    }

    private static final class OkHttpCallback
            implements Callback
    {
        private final SettableFuture<Response> future;

        public OkHttpCallback(SettableFuture<Response> future)
        {
            this.future = requireNonNull(future, "future is null");
        }

        @Override
        public void onFailure(Call call, IOException e)
        {
            future.setException(e);
        }

        @Override
        public void onResponse(Call call, Response response)
                throws IOException
        {
            future.set(response);
        }
    }
}
