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
package com.facebook.presto.spark.execution;

import com.facebook.presto.server.TaskUpdateRequest;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class BatchTaskUpdateRequest
{
    private final TaskUpdateRequest taskUpdateRequest;
    // Map from plan node id to serialized ShuffleReadInfo
    private final Optional<Map<String, byte[]>> shuffleReadInfos;
    private final Optional<byte[]> shuffleWriteInfo;

    @JsonCreator
    public BatchTaskUpdateRequest(
            @JsonProperty("taskUpdateRequest") TaskUpdateRequest taskUpdateRequest,
            @JsonProperty("shuffleReadInfos") Optional<Map<String, byte[]>> shuffleReadInfos,
            @JsonProperty("shuffleWriteInfo") Optional<byte[]> shuffleWriteInfo)
    {
        this.taskUpdateRequest = requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");
        this.shuffleReadInfos = requireNonNull(shuffleReadInfos, "shuffleReadInfos is null");
        this.shuffleWriteInfo = requireNonNull(shuffleWriteInfo, "shuffleWriteInfo is null");
    }

    @JsonProperty
    public TaskUpdateRequest getTaskUpdateRequest()
    {
        return taskUpdateRequest;
    }

    @JsonProperty
    public Optional<Map<String, byte[]>> getShuffleReadInfos()
    {
        return shuffleReadInfos;
    }

    @JsonProperty
    public Optional<byte[]> getShuffleWriteInfo()
    {
        return shuffleWriteInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskUpdateRequest", taskUpdateRequest)
                .add("shuffleReadInfos", shuffleReadInfos)
                .add("shuffleWriteInfo", shuffleWriteInfo)
                .toString();
    }
}
