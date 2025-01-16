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
package com.facebook.presto.operator;

import com.facebook.presto.operator.PageBufferClient.PagesResponse;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

/**
 * All methods in this class should be async
 */
public interface RpcShuffleClient
{
    ListenableFuture<PagesResponse> getResults(long token, DataSize maxResponseSize);

    /**
     * A fire and forget call to issue the ack to the buffer.
     * No need to handle the response; it is ok for a server to miss the ack.
     * {@param nextToken} N represents token N - 1 is to be acknowledged.
     * The implementation needs to guarantee the function is non-blocking and result or failure is not important.
     */
    void acknowledgeResultsAsync(long nextToken);

    /**
     * Close remote buffer
     */
    ListenableFuture<?> abortResults();

    /**
     * Rewrite network related exception to Presto exception
     */
    Throwable rewriteException(Throwable throwable);
}
