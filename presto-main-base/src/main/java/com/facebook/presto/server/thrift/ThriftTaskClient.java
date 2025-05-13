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

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.ThriftBufferResult;
import com.google.common.util.concurrent.ListenableFuture;

// TODO: the client currently only supports exchange; more methods (for /v1/task) should be supported
@ThriftService("ThriftTaskService")
public interface ThriftTaskClient
{
    @ThriftMethod
    ListenableFuture<ThriftBufferResult> getResults(TaskId taskId, OutputBufferId bufferId, long token, long maxSizeInBytes);

    @ThriftMethod
    ListenableFuture<Void> acknowledgeResults(TaskId taskId, OutputBufferId bufferId, long token);

    @ThriftMethod
    ListenableFuture<Void> abortResults(TaskId taskId, OutputBufferId bufferId);
}
