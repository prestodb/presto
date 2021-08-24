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
package com.facebook.presto.execution;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.QueryId;

import java.net.URI;

public interface LocationFactory
{
    URI createQueryLocation(QueryId queryId);

    URI createStageLocation(StageId stageId);

    URI createLocalTaskLocation(TaskId taskId);

    /**
     * TODO: this method is required since not not all RPC call is supported by thrift.
     *     It should be merged into {@code createTaskLocation} once full thrift support is in-place for v1/task
     */
    @Deprecated
    URI createLegacyTaskLocation(InternalNode node, TaskId taskId);

    /**
     * TODO: implement full thrift support for v1/task
     */
    URI createTaskLocation(InternalNode node, TaskId taskId);

    URI createMemoryInfoLocation(InternalNode node);
}
