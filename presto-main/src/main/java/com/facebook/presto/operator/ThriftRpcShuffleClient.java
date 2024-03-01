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

import com.facebook.drift.client.DriftClient;
import com.facebook.drift.transport.client.MessageTooLargeException;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.ThriftBufferResult;
import com.facebook.presto.operator.PageBufferClient.PagesResponse;
import com.facebook.presto.server.thrift.ThriftTaskClient;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.operator.PageBufferClient.PagesResponse.createPagesResponse;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class ThriftRpcShuffleClient
        implements RpcShuffleClient
{
    private final ThriftTaskClient thriftClient;
    private final TaskId taskId;
    private final OutputBufferId outputBufferId;

    public ThriftRpcShuffleClient(DriftClient<ThriftTaskClient> driftClient, URI location)
    {
        requireNonNull(location, "location is null");

        this.thriftClient = requireNonNull(driftClient, "thriftClient is null").get(Optional.of(location.getAuthority()));

        // TODO: refactor the entire LocationFactory interfaces to completely replace URI with more efficient/expressive data structures
        // location format: thrift://{ipAddress}:{thriftPort}/v1/task/{taskId}/results/{bufferId}/
        String[] paths = location.getPath().split("/");
        this.taskId = TaskId.valueOf(paths[3]);
        this.outputBufferId = OutputBufferId.fromString(paths[5]);
    }

    @Override
    public ListenableFuture<PagesResponse> getResults(long token, DataSize maxResponseSize)
    {
        ListenableFuture<ThriftBufferResult> future = thriftClient.getResults(taskId, outputBufferId, token, maxResponseSize.toBytes());
        return Futures.transform(
                future,
                result -> createPagesResponse(
                        result.getTaskInstanceId(),
                        result.getToken(),
                        result.getNextToken(),
                        result.getSerializedPages(),
                        result.isBufferComplete()),
                directExecutor());
    }

    @Override
    public void acknowledgeResultsAsync(long nextToken)
    {
        // no need to handle the future
        thriftClient.acknowledgeResults(taskId, outputBufferId, nextToken);
    }

    @Override
    public ListenableFuture<?> abortResults()
    {
        return thriftClient.abortResults(taskId, outputBufferId);
    }

    @Override
    public Throwable rewriteException(Throwable throwable)
    {
        if (throwable instanceof MessageTooLargeException) {
            return new PageTooLargeException(throwable);
        }
        return throwable;
    }
}
